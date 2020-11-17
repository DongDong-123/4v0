# -*- coding: utf-8 -*-
# @Time    : 2020/5/8 10:43
# @Author  : liudongyang
# @FileName: run_new.py
# @Software: PyCharm
import sys
import os
import time
import zipfile
import datetime

from readConfig import RunConfig, Setting
config = RunConfig()
settings = Setting()

from task_schedule import main1, main8, main9

from loggers import LogInfo

loginfo = LogInfo()
log = loginfo.logger("DEBUG")

if sys.platform == 'linux':
    os.chdir('/home/admin/make_data/tomysql/4v0')
    zip_floder = config.get_linux_data_path()
    if not os.path.exists(zip_floder):
        os.makedirs(zip_floder, exist_ok=False)
elif sys.platform == 'win32':
    zip_floder = config.get_win_data_path()
    if not os.path.exists(zip_floder):
        os.makedirs(zip_floder, exist_ok=False)
else:
    zip_floder=os.getcwd()

current_path = os.getcwd()

def get_parm():
    with open(os.path.join(current_path, 'parm.txt'), 'r', encoding='utf-8') as f:
        res = f.read()

    parm = res.split(',')
    n = int(parm[0])
    t = int(parm[1])
    print('开始序号{}'.format(n))
    print('parm文件交易日期{}'.format(t))

    return n, t


def updtae_parm(n, t):
    with open(os.path.join(current_path, 'parm.txt'), 'w', encoding='utf-8') as f:
        f.write("{},{}".format(n, t))


def zip_file(start_dir, date):
    os.chdir(start_dir)
    start_dir = start_dir  # 要压缩的文件夹路径
    file_news = '{}'.format(date) + '_1.zip'  # 压缩后文件夹的名字
    print(file_news)
    z = zipfile.ZipFile(file_news, 'w', zipfile.ZIP_DEFLATED)
    for dir_path, dir_names, file_names in os.walk(start_dir):
        f_path = dir_path.replace(start_dir, '')  # 这一句很重要，不replace的话，就从根目录开始复制
        f_path = f_path and f_path + os.sep or ''  # 实现当前文件夹以及包含的所有文件的压缩
        # print('f_path', f_path)
        for filename in file_names:
            if date in filename and filename[-3:] == 'csv':
                # print(filename)
                z.write(os.path.join(dir_path, filename), f_path + filename)
                # print('tt', os.path.join(dir_path, filename), f_path + filename)
                os.remove(filename)
            else:
                print('-----------------')
                print(filename)
    z.close()
    with open(os.path.join(start_dir,'{}'.format(date))+"_1.txt", 'w', encoding='utf-8') as f:
        pass
    return file_news


def run1():
    n, t = get_parm()

    start_time = time.time()
    # threads = []
    # for count in range(10):
    #     t = Thread(target=main, args=(count*10, (count+1)*10))
    #     t.start()
    #     threads.append(t)
    # for t in threads:
    #     t.join()
    # -------------------------单线程
    # 数据条数
    o = int(settings.data_num())
    stif_num = int(settings.stif_num())
    num_days = int(settings.num_days())

    for m in range(num_days):
        st = datetime.datetime.strptime(str(t), "%Y%m%d")
        file_date_time = str(st)[:10]
        stif_time = "{}100000".format(t)

        main1(n, n + o, stif_time, file_date_time, stif_num)
        # te()
        n += o
        t += 1
        zip_file(zip_floder, file_date_time)

    updtae_parm(n, t)

    end_time = time.time()
    print(end_time - start_time)  # 13


def run2():
    n, t = get_parm()
    start_time = time.time()
    # threads = []
    # for count in range(10):
    #     t = Thread(target=main, args=(count*10, (count+1)*10))
    #     t.start()
    #     threads.append(t)
    # for t in threads:
    #     t.join()
    # -------------------------单线程
    # o数据条数
    o = int(settings.data_num())
    stif_num = int(settings.stif_num())
    num_days = int(settings.num_days())

    for m in range(num_days):
        st = datetime.datetime.strptime(str(t), "%Y%m%d")
        file_date_time = str(st)[:10]
        stif_time = "{}100000".format(t)

        main8(n, n + o, stif_time, file_date_time, stif_num)
        n += o
        t += 1
        zip_file(zip_floder, file_date_time)

    end_time = time.time()
    print(end_time - start_time)  # 13

    updtae_parm(n, t)


if __name__ == "__main__":
    run1()
    # run2()