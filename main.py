# -*- coding: UTF-8 -*-
from sys import argv
import dask.dataframe as dd
import logging
from datetime import datetime
import time

try:
    data_argv = argv
except IndexError as e:
    logging.error("Run script must with parameter")
    print('Run script must with parameter')
    exit('1')
    raise e

VERSION = 19.0

# ОКРУЖЕНИЕ
PROD = True

if PROD:
    # Путь к файлу для обработки:
    path_to_file_Commercial = "D:\\Export\\DP\\Commercial\\"
    path_to_file_PnL = "D:\\Export\\DP\\PnL\\"
    path_to_file_Calendar_shift = "D:\\Export\\DP\\Calendar_shift\\"
    path_to_file_Efficiency = "D:\\Export\\DP\\Efficiency\\"
    path_to_file_ntz = "D:\\Export\\DP\\ntz\\"
    #  main .log file
    path_to_log = "D:\\HyperLog\\DP\\pyConvert.log"
else:
    path_to_file_Commercial = "C:\\Users\\60001240\\Desktop\\DP\\Commercial\\"
    path_to_file_PnL = "C:\\Users\\60001240\\Desktop\\DP\\PnL\\"
    path_to_file_Calendar_shift = "C:\\Users\\60001240\\Desktop\\DP\\Calendar_shift\\"
    path_to_file_Efficiency = "C:\\Users\\60001240\\Desktop\\DP\\Efficiency\\"
    path_to_file_ntz = "C:\\Users\\60001240\\Desktop\\DP\\ntz\\"
    path_to_log = "pyConvert.log"

# Название файлов по выгрузки
#  Коммерческие показатели
list_name_Commercial = [
    "CE_Commercial_TSC_Day.txt",
    "CE_Commercial_CPC_Month.txt",
    "CE_Commercial_CPC_Month_Plan2.txt",
    "CE_Commercial_CPC_Day.txt",
    "CE_Commercial_NoCPC_Day.txt",
    "CE_Commercial_TSC_Month.txt",
    "CE_Commercial_TSC_Month_Plan2.txt",
    "CE_Commercial_NoCPC_Month.txt",
    "CE_Commercial_NoCPC_Month_Plan2.txt"
]
#  CЧёт эксплуатации
list_name_PnL = [
    "CE_PnL_CPC_Month.txt",
    "CE_PnL_NoCPC_Month.txt"
]
Calendar_shift = [
    "CE_Calendar_shift.txt"
]
# Эффективность
list_name_Efficiency = [
    "CE_Efficiency_Month.txt"
]

# Нетоварные закупки
list_name_ntz = [
    "ntz_month.txt"
]

#  Лог
logging.basicConfig(filename=path_to_log, level=logging.INFO)


# Функция конвертирует данные и проверяет кол-во записей.
def main(filename, path):
    print(filename)
    today = datetime.now()
    start_job = time.time()
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
        print(f' count_txt {count_txt}  == count_csv {count_csv}')
    else:
        print(f' count_txt {count_txt} <> count_csv {count_csv}')
        logging.error("count_csv != count_csv")
        return 1  # 1 не сработало.

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
    return 0  # 0 - всё ОК.


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
            #  Обрабатываем каждую папку отдельно: PnL
        
        if data_argv[1] == 'ntz':
            for file in list_name_ntz:
                main(filename=file, path=path_to_file_PnL)

        # Календарный сдвиг
        if data_argv[1] == 'Calendar_shift':

            for file in Calendar_shift:
                main(filename=file, path=path_to_file_Calendar_shift)

                #  Обрабатываем каждую папку отдельно: Efficiency
        if data_argv[1] == 'Efficiency':
            for file in list_name_Efficiency:
                main(filename=file, path=path_to_file_Efficiency)

        if not data_argv[1] == 'Efficiency' and \
           not data_argv[1] == 'Calendar_shift' and \
           not data_argv[1] == 'PnL' and \
           not data_argv[1] == 'ntz' and \
           not data_argv[1] == 'Commercial':
            logging.error(f'Wrong data_argv {data_argv}')
            print(f'Used to: https://github.com/nvetrov/DP ')
            print('-----------------------------')
            print(f'py .\main.py Commercial')
            print(f'py .\main.py PnL')
            print(f'py .\main.py Calendar_shift')
            print(f'py .\main.py Efficiency')
            print(f'py .\main.py ntz')
            print(f'-------------------------------------')
            exit('1')

    except EOFError as e:
        # print("Caught the EOF error.")
        logging.error("Caught the EOF error")
        raise e
    except IOError as e:
        logging.error("Caught the I/O error")
        raise e
    except IndexError as e:
        logging.error(f'Run script must with parameter')
        print('Run script must with parameter: ')
        exit('1')
        raise e
