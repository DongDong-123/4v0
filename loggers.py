#!coding=utf-8
import logging
import os
import time


class LogInfo:
    def __init__(self):
        pass

    def logger(self, level='INFO'):
        path = os.path.join(os.getcwd(), 'log')
        if not os.path.exists(path):
            os.makedirs(path, mode=0o770)
        file = time.strftime('%Y-%m-%d', time.localtime()) + '.log'
        file_name = os.path.join(path, file)
        if not os.path.exists(file_name):
            open(file_name, 'w', encoding='utf-8')

        logging.basicConfig(level=level, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s', filename=file_name)

        return logging


if __name__ == "__main__":
    logs = LogInfo()
    llg = logs.logger('DEBUG')
    llg.info('test')
