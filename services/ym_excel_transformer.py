import asyncio
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import re

from pandas import ExcelFile, DataFrame
from torchgen.executorch.api.et_cpp import return_type

# Отображать все столбцы
pd.set_option('display.max_columns', None)

# Отображать все строки
pd.set_option('display.max_rows', None)

# Не обрезать содержимое столбцов
pd.set_option('display.max_colwidth', None)

# (Опционально) ширина консоли
pd.set_option('display.width', None)

file_path = 'united_orders_21570113_01-01-2025_31-01-2025.xlsx'
max_order_days_ago = 60

# Список листов, которые используются в отчете для преобразований
SHEETS = ['Сводка', 'Услуги и маржа по заказам', 'Транзакции по заказам и товарам']

# Колонки, которые используются с листа "Услуги и маржа по заказам" и переименование в фикс для кода
fin_cols: dict[str, str] = {
    'Номер заказа': 'Номер заказа'
    , 'Цена продажи (за шт.), ₽': 'Цена продажи (весь чек), ₽'
    , 'Все услуги Маркета за заказы, ₽': 'Все услуги Маркета за заказы, ₽'
    , 'Доход за вычетом услуг Маркета, ₽': 'Доход за вычетом услуг Маркета, ₽'
    , 'Статус заказа': 'Статус заказа'
}

# Колонки, которые используются с листа "Транзакции по заказам и товарам" и переименование в фикс для кода
trans_cols: dict[str, str] = {
    'Номер заказа': 'Номер заказа'
    , 'Дата оформления': 'Дата оформления'
    , 'Ваш SKU': 'Ваш SKU'
    , 'Название товара': 'Название товара'
    , 'Доставлено или возвращено': 'Количество'
    , 'Статус товара': 'Статус товара'
    , 'Цена продажи (за шт.), ₽': 'Цена продажи (за шт.), ₽'
}

# "Статус товара" приводим к 3 используемым бизнесом категориям
goods_status_dict: dict[str, str] = {
    'Доставлен покупателю': 'Доставлен'
    , 'Отменён': 'Отменён'
    , 'Невыкуп принят на складе': 'Отменён'
    , 'Отгружен': 'В работе'
    , 'Невыкуп отправлен': 'Отменён'
    , 'Возврат принят на складе': 'Отменён'
    , 'Невыкуп принят': 'Отменён'
    , 'Невыкуп передан вам': 'Отменён'
    , 'Оформлен': 'В работе'
    , 'Возврат готов к передаче вам': 'Отменён'
    , 'Возврат передан вам': 'Отменён'
    , 'Невыкуп готов к передаче вам': 'Отменён'
    , np.nan: np.nan
}

# "Статус заказа", которые необходимо переименовать
order_status_dict: dict[str, str] = {
    'Заказ отменен до обработки': 'Заказ отменен до обработки'  # использую
    , 'Передан в доставку': 'Передан в доставку'
    , 'Доставлен на пункт выдачи': 'Доставлен на пункт выдачи'
    , 'Невыкуп принят на складе': 'Невыкуп принят на складе'
    , 'Отменен при обработке': 'Отменен при обработке'
    , 'В обработке': 'В обработке'
    , 'Отменен при доставке': 'Отменен при доставке'
    , 'Полный возврат принят на складе': 'Полный возврат принят на складе'
    , 'Доставлен': 'Доставлен'
    , 'Частичный невыкуп принят на складе': 'Частичный невыкуп принят на складе'
}

# колонки для пересчета с учетом коэффициента
coeff_cols = ['Все услуги Маркета за заказы, ₽', 'Доход за вычетом услуг Маркета, ₽']

col_for_result = ['Ваш SKU', 'Количество', 'Статус товара', 'Цена продажи (за шт.), ₽',
                  'Все услуги Маркета за заказы, ₽', 'Доход за вычетом услуг Маркета, ₽']


def _read_excel(file) -> ExcelFile | None:
    try:
        return pd.ExcelFile(file)
    except Exception as e:
        return None


def _miss_required_sheets(check_file: ExcelFile) -> str:
    available_sheets = check_file.sheet_names
    missing_sheets = set(SHEETS) - set(available_sheets)
    if missing_sheets:
        return f'В файле не хватает:\n- Лист: {'\n- Листа: '.join(missing_sheets)}'
    return ''


def _read_report_period(exl_file: ExcelFile, name_sheet: str) -> pd.Series:
    """
        Извлекает даты из первой ячейки с указанного листа Excel-файла.

        Args:
            exl_file (ExcelFile): Открытый Excel-файл.
            name_sheet (str): Название листа, где находится заголовок с датами.

        Returns:
            pd.Series: Серия с датами (если дат не найдено, то пустая серия).

        """
    read_data = exl_file.parse(sheet_name=name_sheet, nrows=1, header=None)
    dates = re.findall(r"\d{2}\.\d{2}\.\d{4}", str(read_data.iloc[0].values[0]))

    return pd.to_datetime(dates, format='%d.%m.%Y')


def _parse_sheet_to_df(exl_file: ExcelFile, name_sheet: str, header: int,
                       need_cols: dict[str, str], col_replacement: str = None,
                       replacement_dict: dict[str, str] = None) -> DataFrame:
    read_data = exl_file.parse(sheet_name=name_sheet, header=header)

    missing_cols = need_cols.keys() - set(read_data.columns)
    if missing_cols:
        print(f'На листа {name_sheet} НЕ ХВАТАЕТ КОЛОНОК: {missing_cols}')
        return pd.DataFrame()

    # оставляем нужные колонки + переименовываем для устойчивости скрипта
    read_data = read_data[list(need_cols.keys())].rename(columns=need_cols)

    #
    if col_replacement is not None and replacement_dict is not None:
        missing_values = set(read_data[col_replacement].unique()) - replacement_dict.keys()
        if missing_values:
            print(f'НЕ ХВАТАЕТ СТАТУСА: {missing_values}')
        read_data[col_replacement] = read_data[col_replacement].replace(replacement_dict)

    return read_data


def _apply_coefficient(df, cols, coeff_col, round_decimals=2):
    df = df.copy()
    df[cols] = df[cols].multiply(df[coeff_col].values, axis=0).round(round_decimals)
    return df


async def ym_excel_transformer(file) -> str:
    xls = _read_excel(file)
    if xls is None:
        return 'Не удалось открыть файл. Проверьте, что это Excel (*.xlsx), не защищен паролем, не в архиве.'

    answer = _miss_required_sheets(xls)
    if answer:
        return answer

    dates_dt = _read_report_period(xls, SHEETS[0])
    if len(dates_dt) >= 2:
        report_period = (min(dates_dt), max(dates_dt))
    else:
        report_period = None

    financials = _parse_sheet_to_df(xls, SHEETS[1], 6, fin_cols,
                                    'Статус заказа', order_status_dict)
    transactions = _parse_sheet_to_df(xls, SHEETS[2], 8, trans_cols,
                                      'Статус товара', goods_status_dict)
    if financials.empty or transactions.empty:
        return 'Формат отчета был изменен. Команде разработки информацию передал. Проинформируем о восстановлении работы сервиса с данным отчетом.'

    transactions = transactions.dropna(subset=['Номер заказа'])
    financials = financials[list(fin_cols.values())].fillna(0.0)

    transactions['Дата оформления'] = pd.to_datetime(
        transactions['Дата оформления'],
        format='%d.%m.%Y',
        errors='coerce'  # NaT, если формат не совпадает
    )

    if report_period is not None:
        start_date = min(report_period) - timedelta(days=max_order_days_ago)
        end_date = max(report_period)
        transactions = transactions[
            (transactions['Дата оформления'] >= start_date) & (transactions['Дата оформления'] <= end_date)]

    to_int = ['Номер заказа', 'Количество']
    transactions[to_int] = transactions[to_int].astype('int64')

    transactions = transactions.merge(financials, on='Номер заказа', how='left')
    transactions = transactions[transactions['Статус заказа'] != 'Заказ отменен до обработки']
    transactions['Стоимость товаров, ₽'] = transactions['Количество'].mul(transactions['Цена продажи (за шт.), ₽'])
    transactions['Коэффициент от чека'] = transactions['Стоимость товаров, ₽'] / transactions[
        'Цена продажи (весь чек), ₽']

    transactions = _apply_coefficient(transactions, coeff_cols, 'Коэффициент от чека')

    final_df = transactions[transactions['Статус товара'] != 'В работе'][col_for_result]

    cost_goods = final_df.groupby('Ваш SKU')['Цена продажи (за шт.), ₽'].agg(
        Минимум='min',
        Максимум='max',
        Среднее='mean'
    ).round(2)

    sku_income = final_df[final_df['Статус товара'] == 'Доставлен'].groupby('Ваш SKU')['Доход за вычетом услуг Маркета, ₽'].sum()

    result_div_and_cancel = final_df.groupby('Ваш SKU').agg(
        Услуги_Маркета=('Все услуги Маркета за заказы, ₽', 'sum'),
        sum_div_and_cancel=('Количество', 'sum')
    )

    cancelled_counts = final_df[
        final_df['Статус товара'] == 'Отменён'
    ].groupby('Ваш SKU').agg(
        Количество_отменённых=('Количество', 'sum')
    )

    merged_df = cost_goods.merge(sku_income, how='left', on='Ваш SKU')
    merged_df = merged_df.merge(result_div_and_cancel, how='left', on='Ваш SKU')
    merged_df = merged_df.merge(cancelled_counts, how='left', on='Ваш SKU')
    merged_df = merged_df.fillna(0)

    merged_df['Процент_отменённых, %'] = (merged_df['Количество_отменённых'] / merged_df['sum_div_and_cancel']).round(4) * 100
    merged_df['Количество доставленных, шт'] = merged_df['sum_div_and_cancel'] - merged_df['Количество_отменённых']


    name_goods = transactions.drop_duplicates(subset=['Ваш SKU'], keep='last')[['Ваш SKU', 'Название товара']].set_index('Ваш SKU')
    merged_df = merged_df.merge(name_goods, how='left', on='Ваш SKU')

    merged_df = merged_df.rename(columns={
        'Минимум': 'Мин. цена, ₽',
        'Максимум': 'Макс. цена, ₽',
        'Среднее': 'Средняя цена, ₽',
        'Доход за вычетом услуг Маркета, ₽': 'Чистый доход, ₽',
        'Услуги_Маркета': 'Комиссия Маркета, ₽',
        'Процент_отменённых, %': 'Доля отмен, %',
        'Количество доставленных, шт': 'Доставлено, шт',
        'Название товара': 'Наименование товара'
    })

    orderliness = [
        'Наименование товара',
        'Мин. цена, ₽',
        'Макс. цена, ₽',
        'Средняя цена, ₽',
        'Доставлено, шт',
        'Чистый доход, ₽',
        'Комиссия Маркета, ₽',
        'Доля отмен, %'
    ]

    merged_df = merged_df[orderliness].sort_values(by='Чистый доход, ₽', ascending=False)

    summary_df = transactions.groupby('Статус товара', observed=True)[['Доход за вычетом услуг Маркета, ₽', 'Все услуги Маркета за заказы, ₽']].sum()
    #
    # with pd.ExcelWriter(file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    #     summary_df.to_excel(writer, sheet_name='Сводка по Статусам', index=True)
    #     merged_df.to_excel(writer, sheet_name='Сводка по SKU', index=True)
    print(merged_df.head(5))
    print(summary_df)

if __name__ == '__main__':
    asyncio.run(ym_excel_transformer(file_path))
