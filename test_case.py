# 1 - Написать код подключения к базе данных postgresql и получения данных в формате pandas DataFrame

# 2 - Загрузить из excel исходный файл в pandas DataFrame

# 2.1 - Обработать данные: Оставить только основную таблицу,
# В "Портфель" заменить нет на экспорт, заполнить пустые данные в направлении кредитования словом "прочее"

# 2.2. - Добавить колонки с 0 "Остаток лимита руб. MM-ГГГГ" и "Плановое Погашение руб. ММ-ГГГГ",
# где ММ-ГГГГ - каждый месяц с текущего дня по 2027 год включительно(с возможностью подставить любой другой год)

# 2.3. -  Агрегировать данные в разрезе: "Портфель", "Наличие субсидии Да/Нет" (вместе),
# только где "Тип Сделки" = 'КредЛиния',
# а данные в колонках - созданные столбцы "Остаток лимита руб. MM-ГГГГ"

# 3 - Выгрузить полученные агрегированные данные в excel на рабочий стол

# 4 - Получить курсы валют по текущему дню с сайта ЦБРФ: "Доллар США", "Евро", "Китайский юань"

import os
import pandas as pd
from pandas import DataFrame
import datetime
from dateutil.relativedelta import relativedelta
from typing import Any
from sqlalchemy import create_engine, text



def get_count_month(input_year: int) -> int:
    """
    Подсчет кол-ва месяцев
    """
    today = datetime.datetime.now().strftime("%m.%Y")
    today = datetime.datetime.strptime(today, "%m.%Y")
    end_data = datetime.datetime.now().strftime(f"%m.{input_year}")
    end_data = datetime.datetime.strptime(end_data, "%m.%Y")
    number_month = (end_data.year - today.year) * 12 + (end_data.month - today.month)
    return number_month


def add_columns(df: DataFrame, number_month: int) -> tuple[DataFrame, list[str]]:
    """
    Добавление колонок: df_dates
    - Остаток лимита руб. MM-ГГГГ
    - Плановое Погашение руб. ММ-ГГГГ
    Получение списка дат в формате ММ-ГГГГ: dates_ost
    """
    today = datetime.datetime.now().strftime("%m.%Y")
    today = datetime.datetime.strptime(today, "%m.%Y")
    df_dates = df.copy(deep=True)
    dates_ost = []
    for elem in range(number_month):
        new_date = today + relativedelta(months=elem)
        new_date = new_date.strftime("%m.%Y")
        dates_ost.append(f"Остаток Погашения руб. {new_date}")
        df_dates[f"Остаток Погашения руб. {new_date}"] = 0
        df_dates[f"План погашения руб. {new_date}"] = 0
    return (df_dates, dates_ost)


def get_aggregate_table(df_dates: DataFrame, dates_ost: list[str]) -> DataFrame:
    """
    Аггрегация данных в разрезе: 'Портфель', 'Наличие субсидии Да/Нет'
    """
    aggregate_inform = df_dates[df_dates["Тип сделки"] == "КредЛиния"]
    aggregate_inform = aggregate_inform.groupby(
        ["Портфель", "Наличие субсидии Да/Нет"]
    )[dates_ost]
    aggregate_inform = aggregate_inform.sum().reset_index()
    return aggregate_inform


def save_to_excel(aggregate_inform: DataFrame) -> None:
    """Сохранение данных в файл excel на рабочий стол"""
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    date_now = datetime.datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    path_to_file = os.path.join(desktop_path, f"report_{date_now}.xlsx")
    aggregate_inform.to_excel(path_to_file, index=False)


def get_cbrf_df(valutes: list[str]) -> DataFrame:
    """
    Получение курсов валют
    """
    url1 = 'http://www.cbr.ru/scripts/XML_daily.asp'
    cbrf_df = pd.read_xml(url1, encoding='cp1251')
    cbrf_df = cbrf_df[cbrf_df['Name'].isin(valutes)]
    cbrf_df['Value'] = cbrf_df['Value'].str.replace(',', '.').astype(float)
    return cbrf_df[["Name", "Value"]]


FILENAME = "данные для задания.xlsx"
SHEET_NAME = "Кредитный портфель"
FINAL_YEAR = 2027

df = pd.read_excel(FILENAME, sheet_name=SHEET_NAME, skiprows=6)

# Заполнение пустых ячеек, и замена данных в ячейках
df.fillna({"Направление кредитования": "прочее"}, inplace=True)
df.replace("нет", {"Портфель": "экспорт"}, inplace=True)

# Добавление необходимого кол-ва колонок
count_month = get_count_month(input_year=FINAL_YEAR)
df_add_col = add_columns(df=df, number_month=count_month)
df_dates = df_add_col[0]
dates_ost = df_add_col[1]

# Агрегирование данных
aggregate_inform = get_aggregate_table(df_dates=df_dates, dates_ost=dates_ost)
aggregate_table = get_aggregate_table(df_dates=df_dates, dates_ost=dates_ost)
save_to_excel(aggregate_inform=aggregate_table)

# Запрос курсов валют
valutes = ['Доллар США', 'Евро', 'Китайский юань']
df_valutes = get_cbrf_df(valutes=valutes)

engine = create_engine('postgresql://admin:admin@localhost:5432/my_db_name')

# Удаление таблицы если она существует
table_name = 'finance_table'
with engine.connect() as conn:
    stmt = text(f"DROP TABLE IF EXISTS {table_name};")
    conn.execute(stmt)
    conn.commit()

df_dates.to_sql(table_name, engine, if_exists='replace', index=False)

# Вывод курса валют в консоль
print(df_valutes)


