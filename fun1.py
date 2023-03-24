import time
import mysql.connector
import os


def fun1(user, password, host, database, table_name, folder_path, sleep_time):
    # 连接到 MySQL 数据库
    cnx = mysql.connector.connect(user=user, password=password, host=host, database=database)

    # 创建游标
    cursor = cnx.cursor()

    # 检查表格是否存在，如果存在就删除它
    drop_table = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_table)

    create_table = f"""CREATE TABLE {table_name} (
                       id INT AUTO_INCREMENT PRIMARY KEY,
                       column_name1 VARCHAR(255),
                       column_name2 VARCHAR(255),
                       column_name3 VARCHAR(255),
                       column_name4 VARCHAR(255)
                       )"""

    cursor.execute(create_table)

    # 初始化行数计数器
    row_counter = 0

    # 处理指定文件夹中的所有txt文件
    for filename in os.listdir(folder_path):
        # 只处理以'.txt'结尾的文件
        if filename.endswith('.txt'):
            # 打开文件
            with open(os.path.join(folder_path, filename), 'r') as f:

                # 遍历每一行并提取第一列数据
                for i, line in enumerate(f):
                    # 判断读取到的行是否为空行
                    if not line.strip():
                        continue  # 跳过本次循环

                    # 处理非空行的数据
                    # 提取行中的数据
                    data = line.strip().split('\t')
                    data1 = data[0]
                    data2 = data[1]
                    data3 = data[2]
                    data4 = data[3]

                    # 插入新数据到数据库
                    add_data = ("INSERT INTO {} "
                                "(column_name1, column_name2, column_name3, column_name4) "
                                "VALUES ('{}','{}','{}','{}')".format(table_name, data1, data2, data3, data4))
                    cursor.execute(add_data)

                    # 增加行数计数器
                    row_counter += 1
                    if row_counter % 1000 == 0:
                        print(f"已插入 {row_counter} 行数据。")

                # 暂停指定时间
                time.sleep(sleep_time)

                # 提交更改到数据库
                cnx.commit()

    # 关闭游标和连接
    cursor.close()
    cnx.close()


# insert_data_to_mysql(user='root', password='123456', host='localhost', database='test',
#                      table_name='test_time6', folder_path='./5s_5000', sleep_time=1)

# fun1(user='root', password='123456', host='localhost', database='test',
#                      table_name='test_time6', folder_path=r'D:\研究生\项目\data\1s_1000', sleep_time=1)


# insert_data_to_mysql(user='root', password='1234', host='192.168.119.128', database='test',
#                      table_name='test_time6', folder_path='D:\\研究生\\项目\\data\\5s_5000', sleep_time=1)
