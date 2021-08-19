import time
import pandas as pd

# Название файлов по выгрузки
list_name_Commercial = ["CE_Commercial_TSC_Day.txt",
                        "CE_Commercial_CPC_Month.txt",
                        "CE_Commercial_CPC_Day.txt",
                        "CE_Commercial_NoCPC_Day.txt",
                        "CE_Commercial_TSC_Month.txt"]

list_name_PnL = ["CE_PnL_CPC_Month.txt", "CE_PnL_NoCPC_Month.txt"]


def main(filename, path):
    print("Start")

    # print(path + filename.replace(".txt", ".csv"))
    df = pd.read_csv(path + filename, delimiter=';', dtype=str)
    df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False)
    # Путь к файлу
    df = pd.read_csv(path + filename, delimiter=';', dtype=str)
    df.to_csv(path + filename.replace(".txt", ".csv"), sep=";", index=False)
    # print(path + filename.replace(".txt", ".csv"))

    del df
    # print("End: " + path + filename.replace(".txt", ".csv"))
    return 0


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start_time = time.time()
    try:
        #  Обрабатываем каждую папку отдельно:Commercial
        for file in list_name_Commercial:
            # run   Commercial
            main(filename=file, path="C:\\Users\\60001240\\Desktop\\DP\\Commercial\\")
        #  Обрабатываем каждую папку отдельно: PnL
        for file in list_name_PnL:
            start_time = time.time()
            main(filename=file, path="C:\\Users\\60001240\\Desktop\\DP\\PnL\\")

        print("---Done. %s seconds ---" % round(time.time() - start_time), 2)
    except EOFError as e:
        print("Caught the EOF error.")
        raise e
    except IOError as e:
        print("Caught the I/O error.")
        raise e


