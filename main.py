import pandas as pd
import logging
from datetime import datetime
import time

VERSION = 1.0
# VERSION = "develop"

if VERSION != "develop":
    # Путь к файлу для обработки:
    path_to_file_Commercial = "D:\\Export\\Tableau\\DP\\Commercial\\"
    path_to_file_PnL = "D:\\Export\\Tableau\\DP\\PnL\\"
    path_to_log = "D:\\Export\\Tableau\DP\\logs\\Commercial_PnL.log"
else:
    path_to_file_Commercial = "C:\\Users\\60001240\\Desktop\\DP\\Commercial\\"
    path_to_file_PnL = "C:\\Users\\60001240\\Desktop\\DP\\PnL\\"
    path_to_log = "Commercial_PnL.log"

# Название файлов по выгрузки
list_name_Commercial = [
    "CE_Commercial_TSC_Day.txt",
    "CE_Commercial_CPC_Month.txt",
    "CE_Commercial_CPC_Day.txt",
    "CE_Commercial_NoCPC_Day.txt",
    "CE_Commercial_TSC_Month.txt"
]
list_name_PnL = [
    "CE_PnL_CPC_Month.txt",
    "CE_PnL_NoCPC_Month.txt"
]
#  Лог
logging.basicConfig(filename="Commercial_PnL.log", level=logging.INFO)


# Функция конвертирует данные и проверяет кол-во записей.
def main(filename, path):
    today = datetime.now()
    start_job = time.time()
    print("Start")

    df = pd.read_csv(path + filename, delimiter=';', dtype=str)
    # count all  lines.
    count_txt = df.shape[0]
    df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False)
    # Путь к файлу
    df = pd.read_csv(path + filename, delimiter=';', dtype=str)
    # Убираю доп. ковычки
    df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False)
    '''Сверка по кол-ву: txt == csv'''
    df_new = pd.read_csv(path + filename, delimiter=';', dtype=str)
    count_csv = df_new.shape[0]

    if count_txt == count_csv:
        print(f' count_txt {count_txt}  совпадает с count_csv {count_csv}')
    else:
        logging.error("count_csv != count_csv")

    #  В ЛОГ
    total_string = format(round(time.time() - start_job, 2))
    now_string = today.strftime("%d/%m/%Y %H:%M:%S")
    output = "Start job: " + now_string + ';' + total_string + ";" + filename.replace(".txt",
                                                                                      ".csv") + ";" + str(
        count_csv)
    logging.info(output)
    print(filename.replace(".txt", ".csv"))
    del df
    del df_new
    # print("End: " + path + filename.replace(".txt", ".csv"))
    return 0


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    start_time = time.time()
    # datetime object containing current date and time

    try:
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        #  Обрабатываем каждую папку отдельно:Commercial
        for file in list_name_Commercial:
            main(filename=file, path=path_to_file_Commercial)
        #  Обрабатываем каждую папку отдельно: PnL
        for file in list_name_PnL:
            main(filename=file, path=path_to_file_PnL)

    except EOFError as e:
        # print("Caught the EOF error.")
        logging.error("Caught the EOF error")
        raise e
    except IOError as e:
        logging.error("Caught the I/O error")
        raise e
