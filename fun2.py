import os
import glob
import time
import numpy as np
import mysql.connector


def connect_db(user, password, host, database):
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)
    return cnx


def create_table(cnx, table_name):
    cursor = cnx.cursor()

    drop_table = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_table)

    create_table = f"""CREATE TABLE {table_name} (
                   id INT AUTO_INCREMENT PRIMARY KEY,
                   file_name VARCHAR(255) NOT NULL,
                   mean_1 FLOAT NOT NULL,
                   mean_2 FLOAT NOT NULL,
                   mean_3 FLOAT NOT NULL,
                   std_1 FLOAT NOT NULL,
                   std_2 FLOAT NOT NULL,
                   std_3 FLOAT NOT NULL,
                   variance_1 FLOAT NOT NULL,
                   variance_2 FLOAT NOT NULL,
                   variance_3 FLOAT NOT NULL,
                   peak_1 FLOAT NOT NULL,
                   peak_2 FLOAT NOT NULL,
                   peak_3 FLOAT NOT NULL,
                   peak_to_peak_1 FLOAT NOT NULL,
                   peak_to_peak_2 FLOAT NOT NULL,
                   peak_to_peak_3 FLOAT NOT NULL,
                   rms1 FLOAT NOT NULL,
                   rms2 FLOAT NOT NULL,
                   rms3 FLOAT NOT NULL,
                   crest_factor1 FLOAT NOT NULL,
                   crest_factor2 FLOAT NOT NULL,
                   crest_factor3 FLOAT NOT NULL
                   
                   )"""

    cursor.execute(create_table)
    cursor.close()


def load_data(folder_path, cnx, table_name, sleep_time):
    cursor = cnx.cursor()

    for file_path in glob.glob(os.path.join(folder_path, '*.txt')):
        data = np.loadtxt(file_path)
        vib_signal = data[:, :3]
        mean = np.mean(vib_signal, axis=0)
        std = np.std(vib_signal, axis=0)
        variance = np.var(vib_signal, axis=0)
        peak = np.max(np.abs(vib_signal), axis=0)
        peak_to_peak = np.max(vib_signal, axis=0) - np.min(vib_signal, axis=0)
        rms = np.sqrt(np.mean(vib_signal ** 2, axis=0))
        crest_factor = peak / rms

        file_name = os.path.basename(file_path)

        insert_query = f"INSERT INTO {table_name} (file_name, mean_1, mean_2, mean_3, std_1, std_2, std_3, " \
                       f"variance_1, variance_2, variance_3, peak_1, peak_2, peak_3, peak_to_peak_1, peak_to_peak_2, " \
                       f"peak_to_peak_3, rms1, rms2, rms3, crest_factor1, crest_factor2, crest_factor3) VALUES (%s, " \
                       f"%s, %s, %s, %s, %s, " \
                       f"%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        insert_data = (
            file_name, mean[0], mean[1], mean[2], std[0], std[1], std[2], variance[0], variance[1], variance[2],
            peak[0], peak[1], peak[2], peak_to_peak[0], peak_to_peak[1], peak_to_peak[2], rms[0], rms[1], rms[2],
            crest_factor[0], crest_factor[1], crest_factor[2])

        cursor.execute(insert_query, insert_data)
        cnx.commit()

        time.sleep(sleep_time)

    cursor.close()


def fun2(user, password, host, database, table_name, folder_path, sleep_time):
    cnx = connect_db(user, password, host, database)
    create_table(cnx, table_name)
    load_data(folder_path, cnx, table_name, sleep_time)
    cnx.close()


# if __name__ == '__fun2__':
fun2(user='root', password='123456', host='localhost', database='test', table_name='vibration_data',
     folder_path='D:\\研究生\\项目\\data\\1s_1000', sleep_time=5)
