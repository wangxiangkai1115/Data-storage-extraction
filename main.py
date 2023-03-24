from threading import Thread
from fun1 import fun1
from fun2 import fun2

# 定义两个线程
thread1 = Thread(target=fun1, args=('root', '123456', 'localhost', 'test', 'test_time6', r'D:\研究生\项目\data\1s_1000', 1))
thread2 = Thread(target=fun2, args=('root', '123456', 'localhost', 'test', 'vibration_data', r'D:\研究生\项目\data\1s_1000', 1))

# 启动两个线程
thread1.start()
thread2.start()

# 等待两个线程结束
thread1.join()
thread2.join()
