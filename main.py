# -*- coding: UTF-8 -*-
from sys import argv
import dask.dataframe as dd
import logging
from datetime import datetime
import time

data_argv = argv
try:
    # Commercial_argv, PnL_argv, Calendar_shift_argv = argv
    data_argv = argv
except ValueError:
    print('ValueError')


print(data_argv[1])

VERSION = 1.8
# VERSION = "develop"

if VERSION != "develop":
    # Путь к файлу для обработки:
    path_to_file_Commercial = "D:\\Export\\Tableau\\DP\\Commercial\\"
    path_to_file_PnL = "D:\\Export\\Tableau\\DP\\PnL\\"
    path_to_file_Calendar_shift = "D:\\Export\\Tableau\\DP\\Calendar_shift\\"
    #  main .log file
    path_to_log = "D:\\HyperLog\\DP\\pyConvert.log"
else:
    path_to_file_Commercial = "C:\\Users\\60001240\\Desktop\\DP\\Commercial\\"
    path_to_file_PnL = "C:\\Users\\60001240\\Desktop\\DP\\PnL\\"
    path_to_file_Calendar_shift = "C:\\Users\\60001240\\Desktop\\DP\\Calendar_shift\\"
    path_to_log = "pyConvert.log"

# Название файлов по выгрузки
list_name_Commercial = [
    "CE_Commercial_TSC_Day.txt",
    "CE_Commercial_CPC_Month.txt",
    "CE_Commercial_CPC_Day.txt",
    "CE_Commercial_NoCPC_Day.txt",
    "CE_Commercial_TSC_Month.txt",
    "CE_Commercial_NoCPC_Month.txt"
]

list_name_PnL = [
    "CE_PnL_CPC_Month.txt",
    "CE_PnL_NoCPC_Month.txt"
]
Calendar_shift = [
    "CE_Calendar_shift.txt"
]

#  Лог
logging.basicConfig(filename=path_to_log, level=logging.INFO)


# Функция конвертирует данные и проверяет кол-во записей.
def main(filename, path):
    today = datetime.now()
    start_job = time.time()
    print(filename)
    # Проверка типа поля DATA -> INT в CE_Calendar_shift.
    if filename == "CE_Calendar_shift.txt":
        df = dd.read_csv(path + filename, delimiter=';', encoding='1251')
        # Конвертируем в INT
        df[df.columns[14]] = df[df.columns[14]].astype('int64')
        # Конвертируем в INT название столбца, чтобы не потерялось первое значение.
        df = df.rename(columns={df.columns[14]: int(float(df.columns[14]))})
        count_txt = df.shape[0].compute() + 1
        df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False, single_file=True)
    else:
        df = dd.read_csv(path + filename, delimiter=';', dtype=str)
        # count all  lines.
        count_txt = df.shape[0].compute() + 1
        df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False, single_file=True)
        # Путь к файлу
        df = dd.read_csv(path + filename, delimiter=';', dtype=str)
        # Убираю доп. ковычки
        df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False, single_file=True)
    '''Сверка по кол-ву: txt == csv'''
    df_new = dd.read_csv(path + filename, delimiter=';', dtype=str)
    count_csv = df_new.shape[0].compute() + 1
    if count_txt == count_csv:
        print(f' count_txt {count_txt}  совпадает с count_csv {count_csv}')
    else:
        logging.error("count_csv != count_csv")

    #  В ЛОГ
    total_string = format(round(time.time() - start_job, 2))
    now_string = today.strftime("%d/%m/%Y %H:%M:%S")
    output = now_string + ';' + total_string + ";" + filename.replace(".txt",
                                                                      ".csv") + ";" + str(
        count_csv)
    # Add line to pyConvert.log
    logging.info(output)
    print(filename.replace(".txt", ".csv"))
    del df
    del df_new
    # print("End: " + path + filename.replace(".txt", ".csv"))
    # console
    return data_argv[1]


if __name__ == '__main__':
    start_time = time.time()
    try:
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        # Обрабатываем каждую папку отдельно:Commercial
        if data_argv[1] == 'Commercial':
            for file in list_name_Commercial:
                main(filename=file, path=path_to_file_Commercial)

        #  Обрабатываем каждую папку отдельно: PnL
        if data_argv[1] == 'PnL':
            for file in list_name_PnL:
                main(filename=file, path=path_to_file_PnL)

        # Календарный сдвиг
        if data_argv[1] == 'Calendar_shift':

            for file in Calendar_shift:
                main(filename=file, path=path_to_file_Calendar_shift)
        print(time.time() - start_time)
    except EOFError as e:
        # print("Caught the EOF error.")
        logging.error("Caught the EOF error")
        raise e
    except IOError as e:
        logging.error("Caught the I/O error")
        raise e
