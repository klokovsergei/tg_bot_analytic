import asyncio
from datetime import date

import pandas as pd
import numpy as np
import re

from pandas import ExcelFile, DataFrame

file_path = 'united_orders_21570113_01-01-2025_31-01-2025.xlsx'

# Список листов, которые используются в отчете для преобразований
SHEETS = ['Сводка', 'Услуги и маржа по заказам', 'Транзакции по заказам и товарам']

# Колонки, которые используются с листа "Услуги и маржа по заказам" и переименование в фикс для кода
fin_cols = {
    'Номер заказа': 'Номер заказа'
    , 'Цена продажи (за шт.), ₽': 'Цена продажи (весь чек), ₽'
    , 'Все услуги Маркета за заказы, ₽': 'Все услуги Маркета за заказы, ₽'
    , 'Доход за вычетом услуг Маркета, ₽': 'Доход за вычетом услуг Маркета, ₽'
    , 'Статус заказа': 'Статус заказа'
}

# Колонки, которые используются с листа "Транзакции по заказам и товарам" и переименование в фикс для кода
trans_cols = {
    'Номер заказа': 'Номер заказа'
    , 'Дата оформления': 'Дата оформления'
    , 'Ваш SKU': 'Ваш SKU'
    , 'Название товара': 'Название товара'
    , 'Доставлено или возвращено': 'Количество'
    , 'Статус товара': 'Статус товара'
    , 'Цена продажи (за шт.), ₽': 'Цена продажи (за шт.), ₽'
}

# "Статус товара" приводим к 3 используемым бизнесом категориям
goods_status_dict = {
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


def _read_report_period(exl_file: ExcelFile, name_sheet: str) -> list[str]:
    """
        Извлекает период отчета в виде двух дат (начало и конец) с указанного листа Excel-файла.

        Args:
            excel_file (ExcelFile): Открытый Excel-файл.
            sheet_name (str): Название листа, где находится заголовок с датами.

        Returns:
            tuple[date, date]: Кортеж с начальной и конечной датами отчётного периода.
            None: Не удалось считать 2 даты

        Raises:
            ValueError: Если не удаётся извлечь две даты из первой строки листа.
        """
    read_data = exl_file.parse(sheet_name=name_sheet, nrows=1, header=None)
    dates = re.findall(r"\d{2}\.\d{2}\.\d{4}", str(read_data.iloc[0].values[0]))
    dates_dt = pd.to_datetime(dates, format='%d.%m.%Y')

    return [dt.date() for dt in dates_dt]


def _services_margin_to_df(exl_file: ExcelFile, name_sheet: str) -> DataFrame:
    read_data = exl_file.parse(sheet_name=name_sheet, header=6)
    missing_fin_cols = fin_cols.keys() - set(read_data.columns)

    if missing_fin_cols:
        # передать logger 'НЕ ХВАТАЕТ КОЛОНОК: {missing_fin_cols}'
        return pd.DataFrame()

    return read_data


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

    financials = _services_margin_to_df(xls, SHEETS[1])
    if financials.empty:
        return 'Формат листа "Услуги и маржа по заказам" был изменен. Команде разработки информацию передал. Проинформируем о восстановлении работы сервиса с данным отчетом.'




    print(financials)


    #
    # if not('Заказ отменен до обработки' in set(financials['Статус заказа'].unique())):
    #     financials['Статус заказа'] = financials['Статус заказа'].replace(
    #         to_replace='Заказ отменен до обработки' # Новое формулировка статуса
    #         ,value='Заказ отменен до обработки' # как настроено в данном скрипте
    #     )
    #     print(financials['Статус заказа'].unique())
    #
    #
    # transactions = pd.read_excel(file, sheet_name=sheets[2], header=8)

    #

    #
    # missing_trans_cols = trans_cols.keys() - set(transactions.columns)
    # if missing_trans_cols:
    #     print(f'НЕ ХВАТАЕТ КОЛОНОК: {missing_trans_cols}', end='\n\n')
    #     print(transactions.columns)
    #
    # missing_goods_status = set(transactions['Статус товара'].unique()) - goods_status_dict.keys()
    # if missing_goods_status:
    #     print(f'НЕ ХВАТАЕТ СТАТУСА: {missing_goods_status}', end='\n\n')
    #     print(transactions['Статус товара'].unique())
    #
    #

    #
    #
    # # оставляем необходимые для работы колонки и переименовываем
    # transactions = transactions[list(trans_cols.keys())].rename(columns=trans_cols)
    # financials = financials[list(fin_cols.keys())].rename(columns=fin_cols).fillna(0.0)
    # # Заполняем NaN нулями, чтобы мат. функции нормально отрабатывали
    #
    # transactions = transactions.dropna(subset=['Номер заказа']) # заказ без номера
    #
    # # преобразование форматов
    # transactions['Дата оформления'] = pd.to_datetime(
    #     transactions['Дата оформления'],
    #     format='%d.%m.%Y',
    #     errors='coerce' # NaT, если формат не совпадает
    # )
    # transactions = transactions.copy()
    # financials = financials.copy()
    # transactions['Количество'] = transactions['Количество'].fillna(0)
    # financials['Статус заказа'] = financials['Статус заказа'].astype('category')
    #
    #
    # # Преобразовать в целое число
    # to_int = ['Номер заказа'
    #           ,'Количество']
    #
    # transactions[to_int] = transactions[to_int].astype('int64')
    #
    # # Соединим транзации и расходы на услуги Маркета
    # transactions = transactions.merge(financials, on='Номер заказа', how='left')
    #
    #
    # # Удаляем записи со статусом 'Заказ отменен до обработки', так как в них нет смысла для бизнеса
    # transactions = transactions[transactions['Статус заказа'] != 'Заказ отменен до обработки']
    #
    # # сокращаем статусы товаров до 3 - "доставлен", "отменен", "в работе"
    # transactions['Статус товара'] = (
    #     transactions['Статус товара']
    #     .replace(goods_status_dict)
    #     .astype('category')
    # )
    #
    #
    # # Добавим колонку "Стоимость товаров, ₽" = "Количество" * "Цена продажи, ₽"
    # transactions['Стоимость товаров, ₽'] = transactions['Количество'].mul(transactions['Цена продажи (за шт.), ₽'])
    #
    # # Коэффициент от чека (доля стоимости товаров от продажной цены)
    # transactions['Коэффициент от чека'] = transactions['Стоимость товаров, ₽'] / transactions['Цена продажи (весь чек), ₽']
    #
    # # пересчитываем колонки с учетом коэффициента доли в чеке
    # coeff_cols = ['Все услуги Маркета за заказы, ₽', 'Доход за вычетом услуг Маркета, ₽']
    #
    # def apply_coefficient(df, cols, coeff_col, round_decimals=2):
    #     df = df.copy()
    #     df[cols] = df[cols].multiply(df[coeff_col].values, axis=0).round(round_decimals)
    #     return df
    #
    # transactions = apply_coefficient(transactions, coeff_cols, 'Коэффициент от чека')
    #
    # col_for_result = ['Ваш SKU', 'Количество', 'Статус товара', 'Цена продажи (за шт.), ₽', 'Все услуги Маркета за заказы, ₽',
    #                   'Доход за вычетом услуг Маркета, ₽']
    #
    # final_df = transactions[transactions['Статус товара'] != 'В работе'][col_for_result]
    #
    # cost_goods = final_df.groupby('Ваш SKU')['Цена продажи (за шт.), ₽'].agg(
    #     Минимум='min',
    #     Максимум='max',
    #     Среднее='mean'
    # ).round(2)
    #
    # sku_income = final_df[final_df['Статус товара'] == 'Доставлен'].groupby('Ваш SKU')['Доход за вычетом услуг Маркета, ₽'].sum()
    #
    # result_div_and_cancel = final_df.groupby('Ваш SKU').agg(
    #     Услуги_Маркета=('Все услуги Маркета за заказы, ₽', 'sum'),
    #     sum_div_and_cancel=('Количество', 'sum')
    # )
    #
    # cancelled_counts = final_df[
    #     final_df['Статус товара'] == 'Отменён'
    # ].groupby('Ваш SKU').agg(
    #     Количество_отменённых=('Количество', 'sum')
    # )
    #
    # merged_df = cost_goods.merge(sku_income, how='left', on='Ваш SKU')
    # merged_df = merged_df.merge(result_div_and_cancel, how='left', on='Ваш SKU')
    # merged_df = merged_df.merge(cancelled_counts, how='left', on='Ваш SKU')
    # merged_df = merged_df.fillna(0)
    #
    # merged_df['Процент_отменённых, %'] = (merged_df['Количество_отменённых'] / merged_df['sum_div_and_cancel']).round(4) * 100
    # merged_df['Количество доставленных, шт'] = merged_df['sum_div_and_cancel'] - merged_df['Количество_отменённых']
    #
    #
    # name_goods = transactions.drop_duplicates(subset=['Ваш SKU'], keep='last')[['Ваш SKU', 'Название товара']].set_index('Ваш SKU')
    # merged_df = merged_df.merge(name_goods, how='left', on='Ваш SKU')
    #
    # merged_df = merged_df.rename(columns={
    #     'Минимум': 'Мин. цена, ₽',
    #     'Максимум': 'Макс. цена, ₽',
    #     'Среднее': 'Средняя цена, ₽',
    #     'Доход за вычетом услуг Маркета, ₽': 'Чистый доход, ₽',
    #     'Услуги_Маркета': 'Комиссия Маркета, ₽',
    #     'Процент_отменённых, %': 'Доля отмен, %',
    #     'Количество доставленных, шт': 'Доставлено, шт',
    #     'Название товара': 'Наименование товара'
    # })
    #
    # orderliness = [
    #     'Наименование товара',
    #     'Мин. цена, ₽',
    #     'Макс. цена, ₽',
    #     'Средняя цена, ₽',
    #     'Доставлено, шт',
    #     'Чистый доход, ₽',
    #     'Комиссия Маркета, ₽',
    #     'Доля отмен, %'
    # ]
    #
    # merged_df = merged_df[orderliness].sort_values(by='Чистый доход, ₽', ascending=False)
    #
    # summary_df = transactions.groupby('Статус товара', observed=True)[['Доход за вычетом услуг Маркета, ₽', 'Все услуги Маркета за заказы, ₽']].sum()
    #
    # with pd.ExcelWriter(file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    #     summary_df.to_excel(writer, sheet_name='Сводка по Статусам', index=True)
    #     merged_df.to_excel(writer, sheet_name='Сводка по SKU', index=True)


if __name__ == '__main__':
    asyncio.run(ym_excel_transformer(file_path))
