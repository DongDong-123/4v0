import configparser
import os

curPath = os.path.dirname(os.path.realpath(__file__))
cfgPath = os.path.join(curPath, "config.ini")


class BaseConfig:
    def __init__(self):
        self.conf = configparser.ConfigParser()
        self.conf.read(cfgPath, encoding='utf-8')



class ReadConfig:
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read(cfgPath, encoding='utf-8')

    def get_user(self):
        return self.cfg.get("mysql", "USER")

    def get_password(self):
        return self.cfg.get("mysql", "PASSWORD")

    def get_db(self):
        return self.cfg.get("mysql", "DB")

    def get_host(self):
        return self.cfg.get("mysql", "HOST")

    def get_port(self):
        return self.cfg.get("mysql", "PORT")


class Setting(BaseConfig):
    def get_num(self):
        """设置多少条数据存储一次，若格式错误，默认10000条"""
        temp = self.conf.get('setting', 'SAVENUM')
        if temp.isalnum():
            save_num = int(temp)
        else:
            save_num = 10000
        return save_num

    def data_num(self):
        """数据数量"""
        temp = self.conf.get('setting', 'DATANUM')
        return temp

    def stif_num(self):
        temp = self.conf.get('setting', 'STIFNUM')
        return temp

    def num_days(self):
        temp = self.conf.get('setting', 'NUMDAYS')
        return temp


class RunConfig:
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read(cfgPath, encoding='utf-8')

    def get_write_num(self):
        return self.cfg.get("data", "write_number")

    def get_password(self):
        return self.cfg.get("mysql", "PASSWORD")

    def get_db(self):
        return self.cfg.get("mysql", "DB")

    def get_host(self):
        return self.cfg.get("mysql", "HOST")

    def get_port(self):
        return self.cfg.get("mysql", "PORT")

    def get_win_data_path(self):
        return self.cfg.get("path", "WIN_DATAPATH")

    def get_linux_data_path(self):
        return self.cfg.get("path", "LINUX_DATAPATH")

class SendEmailConfig:
    def __init__(self):
        self.cfg = configparser.ConfigParser()
        self.cfg.read(cfgPath, encoding='utf-8')

    def get_smtpserver(self):
        return self.cfg.get("EMAIL", "SMTPSERVER")

    def get_user(self):
        return self.cfg.get("EMAIL", "USER")

    def get_password(self):
        return self.cfg.get("EMAIL", "PASSWORD")

    def get_sender(self):
        return self.cfg.get("EMAIL", "SENDER")

    def get_receiver(self):
        return eval(self.cfg.get("EMAIL", "RECEIVER"))


if __name__ == "__main__":
    res = ReadConfig()
    # print(res.redis_host())
    # print(res.redis_password())