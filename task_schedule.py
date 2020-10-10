# -*- coding: utf-8 -*-
# @Time    : 2020/5/8 9:58
# @Author  : liudongyang
# @FileName: task_schedule.py
# @Software: PyCharm
import time
import threading
import os
import csv
from create_data_new import Org, Person, CommnTable
from save_to_mysql import Save_MySQL
from save_to_csv import write_to_csv, write_to_csv_more
from loggers import LogInfo

loginfo = LogInfo()
log = loginfo.logger("DEBUG")
load_stif = "ctif_id,ctif_tp,client_tp,smid,ctnm,citp,citp_ori,citp_nt,ctid,cbat,cbac,cabm,ctat,ctac,cpin,cpba,cpbn,ctip,tstm,cttp,tsdr,crpp,crtp,crat,tcif_id,tcnm,tsmi,tcit,tcit_ori,tcit_nt,tcid,tcat,tcba,tcbn,tctt,tcta,tcpn,tcpa,tpbn,tcip,tmnm,bptc,pmtc,ticd,busi_type,trans_type,pos_dev_id,trans_stat,bank_stat,mer_prov,mer_area,pos_prov,pos_area,mer_unit,extend1,iofg,trans_channel,ctmac,balance,acc_flag,ctid_edt,tran_flag,trans_order,trans_cst_type,crat_u,crat_c,trans_way,agency_ctnm,agency_citp,agency_ctid,agency_country,runtime"
load_person = "busi_reg_no,ctnm,cten,client_tp,account_tp,busi_type,smid,citp,citp_ori,citp_nt,ctid,ctid_edt,sex,country,nation,birthday,education,ctvc,picm,ficm,marriage,ceml,rgdt,cls_dt,remark,indu_code,stat_flag_ori,stat_flag,mer_prov,mer_city,mer_area,address,tel,mer_unit,is_line,certification,cer_num,con_acc_name,bord_flag,web_info,con_nation,bind_card,ip_code,mac_info,self_acc_no,acc_type1,bank_acc_name,reals,batch_pay,statement_type,runtime"
load_org = "busi_reg_no,ctnm,cten,client_tp,account_tp,busi_type,smid,citp,citp_ori,ctid,ctid_edt,citp_nt,id_type,org_no,linkman,linktel,linkjob,linkmail,linkphone,ceml,ctvc,crnm,crit,crit_ori,crit_nt,crid,crid_edt,rgdt,cls_dt,scale,country,crp_type,fud_date,reg_cptl,remark_ctvc,agency_ctnm,agency_citp,agency_ctid,agency_edt,remark,indu_code,stat_flag_ori,stat_flag,mer_prov,mer_city,mer_area,address,tel,mer_unit,is_line,certification,cer_num,con_acc_name,bord_flag,web_info,con_nation,majority_shareholder_ctnm,majority_shareholder_citp,majority_shareholder_citp_ori,majority_shareholder_ctid,majority_shareholder_edt,reg_cptl_code,bind_card,ip_code,mac_info,self_acc_no,acc_type1,bank_acc_name,reals,complex,clear,batch_pay,statement_type,runtime"
# load_org = "busi_reg_no,ctnm,cten,client_tp,account_tp,busi_type,smid,citp,citp_ori,ctid,ctid_edt,citp_nt,id_type,org_no,linkman,linktel,linkjob,linkmail,linkphone,ceml,ctvc,crnm,crit,crit_ori,crit_nt,crid,crid_edt,rgdt,scale,country,crp_type,fud_date,reg_cptl,remark_ctvc,agency_ctnm,agency_citp,agency_ctid,agency_edt,remark,indu_code,stat_flag_ori,stat_flag,mer_prov,mer_city,mer_area,address,tel,mer_unit,is_line,certification,cer_num,con_acc_name,bord_flag,web_info,con_nation,majority_shareholder_ctnm,majority_shareholder_citp,majority_shareholder_citp_ori,majority_shareholder_ctid,majority_shareholder_edt,reg_cptl_code,bind_card,ip_code,mac_info,self_acc_no,acc_type1,bank_acc_name,reals,complex,clear,batch_pay,statement_type,runtime"
load_cert = "ctif_id,ctif_tp,citp,citp_ori,citp_nt,ctid,iss_unt,address,ctid_edt,iss_dt,iss_ctry,is_rp,runtime"
load_address = "ctif_id,ctif_tp,address_tp,address,ctry,prvc,city,area,postcode,exp_dt,is_rp,runtime"
load_tel = 'ctif_id,ctif_tp,tel_tp,tel,is_rp,runtime'
load_relation = "ctif_id,ctif_tp,rel_tp,rel_layer,rel_ctif,rel_cstp,rel_name,rcnt,citp,citp_ori,ctid,citp_nt,hold_per,hold_amt,ctid_edt,rel_prov,rel_city,rel_area,rear,retl,runtime"
load_pact = "ctif_id,ctif_tp,act_tp,act_cd,act_typ,act_limit,is_self_acc,sales_name,cst_sex,nation,occupation,id_type,id_type_ori,id_no,id_deadline,contact,address,sales_flag,bind_mob,mer_unit,cls_dt,rgdt,cls_stat,runtime"
load_bact = "ctif_id,ctif_tp,act_tp,act_flag,act_cd,cabm,pay_id,is_self_acc,bank_acc_name,mer_unit,runtime"


def make_person(num, stiftime, file_time, stif_num, tosql=None):
    person = Person(tosql)
    t_stan_person, person_contect = person.make_stan_person(num)
    common = CommnTable(t_stan_person, person.ctif_tp, tosql)
    t_stan_cert, cert_contect = common.make_stan_cert()
    t_stan_address, addr_contect = common.make_stan_address()
    t_stan_tel, tel_contect = common.make_stan_tel()
    t_stan_relation, rela_contect = common.make_stan_relation()
    t_stan_pact, pact_contect = common.make_stan_pact()
    t_stan_bact, bact_contect = common.make_stan_bact()
    if person.tosql:
        person_contect.append(file_time)
        cert_contect.append(file_time)
        addr_contect.append(file_time)
        tel_contect.append(file_time)
        rela_contect.append(file_time)
        pact_contect.append(file_time)
        bact_contect.append(file_time)
    stifs = []
    if stif_num and t_stan_person['stat_flag'] == 'n' and isinstance(stif_num, int):
        for num in range(stif_num):
            t_stan_stif, stif_contect = common.make_stan_stif(stiftime)
            if person.tosql:
                stif_contect.append(file_time)
            stifs.append(stif_contect)
            # print(t_stan_stif)
    #
    # print(t_stan_person)
    # print(t_stan_cert)
    # print(t_stan_address)
    # print(t_stan_tel)
    # print(t_stan_relation)
    # print(t_stan_pact)
    # print(t_stan_bact)
    # return (t_stan_person, t_stan_cert, t_stan_address, t_stan_tel, t_stan_relation, t_stan_pact, t_stan_bact, stifs)
    return (person_contect, cert_contect, addr_contect, tel_contect, rela_contect, pact_contect, bact_contect, stifs)


def make_org(num, stiftime, file_time, stif_num, tosql=None):
    org = Org(tosql)
    t_stan_org, org_contect = org.make_stan_org(num)
    common = CommnTable(t_stan_org, org.ctif_tp, tosql)
    t_stan_cert, cert_contect = common.make_stan_cert()
    t_stan_address, addr_contect = common.make_stan_address()
    t_stan_tel, tel_contect = common.make_stan_tel()
    t_stan_relation, rela_contect = common.make_stan_relation()
    t_stan_pact, pact_contect = common.make_stan_pact()
    t_stan_bact, bact_contect = common.make_stan_bact()
    if org.tosql:
        org_contect.append(file_time)
        cert_contect.append(file_time)
        addr_contect.append(file_time)
        tel_contect.append(file_time)
        rela_contect.append(file_time)
        pact_contect.append(file_time)
        bact_contect.append(file_time)
    # print(bact_contect)
    stifs = []
    if stif_num and t_stan_org['stat_flag'] == 'n' and isinstance(stif_num, int):
        for num in range(stif_num):
            t_stan_stif, stif_contect = common.make_stan_stif(stiftime)
            if org.tosql:
                stif_contect.append(file_time)
            stifs.append(stif_contect)
            # print(t_stan_stif)
    # print(t_stan_org)
    # print(t_stan_cert)
    # print(t_stan_address)
    # print(t_stan_tel)
    # print(t_stan_relation)
    # print(t_stan_pact)
    # print(t_stan_bact)
    # print(t_stan_stif)

    # return (t_stan_org, t_stan_cert, t_stan_address, t_stan_tel, t_stan_relation, t_stan_pact, t_stan_bact, stifs)
    return (org_contect, cert_contect, addr_contect, tel_contect, rela_contect, pact_contect, bact_contect, stifs)


def main1(begin, end, stiftime, file_time, stif_num):
    """
    生成数据，写入文件
    :param begin:
    :param end:
    :param stiftime: 交易日期
    :param file_time: 写入文件名的日期
    :param stif_num: 每个客户交易数量
    :return:
    """
    temp_person = []
    temp_org = []
    temp_cert = []
    temp_address = []
    temp_tel = []
    temp_pact = []
    temp_bact = []
    temp_relation = []
    temp_stif = []

    for num in range(begin, end):
        persons, cert, address, tel, relation, pact, bact, stif1 = make_person(num, stiftime, file_time, stif_num)
        temp_person.append(persons)
        temp_cert.append(cert)
        temp_address.append(address)
        temp_tel.append(tel)
        temp_pact.append(pact)
        temp_bact.append(bact)
        temp_relation.append(relation)
        temp_stif.extend(stif1)
        orgs, cert2, address2, tel2, relation2, pact2, bact2, stifs2 = make_org(num, stiftime, file_time, stif_num)

        temp_org.append(orgs)
        temp_cert.append(cert2)
        temp_address.append(address2)
        temp_tel.append(tel2)
        temp_pact.append(pact2)
        temp_bact.append(bact2)
        temp_relation.append(relation2)
        temp_stif.extend(stifs2)

        parms = ["load_org", "load_person", "load_cert", "load_address", "load_tel", "load_pact", "load_bact",
                 "load_relation", "load_stif"]
        parm2 = [temp_org, temp_person, temp_cert, temp_address, temp_tel, temp_pact, temp_bact, temp_relation,
                 temp_stif]
        name = ["t_stan_org", "t_stan_person", "t_stan_cert", "t_stan_address", "t_stan_tel", "t_stan_pact", "t_stan_bact",
                "t_stan_relation", "t_stan_stif"]
        if num % 50 == 0 and len(temp_org) != 0:
            threads = []
            for inde, elem in enumerate(parm2):
                log.info('开始写入{}, 编号num={}, 数量={}'.format(parms[inde], num, len(temp_person)))
                print('{}, 开始写入{}, 编号num={}, 数量={}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                                           parms[inde], num, len(temp_person)))
                file_name = parms[inde].split('_')[-1] + "_" + file_time + '.csv'
                t = threading.Thread(target=write_to_csv_more, args=(file_name, elem))
                t.start()
                threads.append(t)

            for tr in threads:
                tr.join()

            for li in parm2:
                log.info('开始清理数据……')
                print('开始清理数据……')
                li.clear()


def main5(begin, end, stiftime, file_time, stif_num):

    """
    生成数据，写入mysql
    :param begin:
    :param end:
    :param stiftime: 交易日期
    :param file_time: 写入文件名的日期
    :param stif_num: 每个客户交易数量
    :return:
    """
    temp_person = []
    temp_org = []
    temp_cert = []
    temp_address = []
    temp_tel = []
    temp_pact = []
    temp_bact = []
    temp_relation = []
    temp_stifs = []

    for num in range(begin, end):
        persons, cert, address, tel, relation, pact, bact, stifs = make_person(num, stiftime, file_time, stif_num, tosql=1)
        temp_person.append(persons)
        temp_cert.append(cert)
        temp_address.append(address)
        temp_tel.append(tel)
        temp_pact.append(pact)
        temp_bact.append(bact)
        temp_relation.append(relation)
        temp_stifs.extend(stifs)
        orgs, cert2, address2, tel2, relation2, pact2, bact2, stifs2 = make_org(num, stiftime, file_time, stif_num, tosql=1)

        temp_org.append(orgs)
        temp_cert.append(cert2)
        temp_address.append(address2)
        temp_tel.append(tel2)
        temp_pact.append(pact2)
        temp_bact.append(bact2)
        temp_relation.append(relation2)
        temp_stifs.extend(stifs2)

        parms = ["load_org", "load_person", "load_cert", "load_address", "load_tel", "load_pact", "load_bact", "load_relation", "load_stif"]
        parm2 = [temp_org, temp_person, temp_cert, temp_address, temp_tel, temp_pact, temp_bact, temp_relation, temp_stifs]

        if num % 100 == 0 and len(temp_relation) != 0:
            threads = []
            for inde, elem in enumerate(parm2):
                log.info('开始写入{}, 编号num={}, 数量={}'.format(parms[inde], num, len(temp_person)))
                print('{}, 开始写入{}, 编号num={}, 数量={}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), parms[inde], num, len(temp_person)))
                connect = Save_MySQL()
                t = threading.Thread(target=connect.save2, args=(parms[inde], eval(parms[inde]), elem))
                t.start()
                threads.append(t)

            for tr in threads:
                tr.join()

            for li in parm2:
                log.info('开始清理数据……')
                print('开始清理数据……')
                li.clear()


class ReadFile:
    """
    读取文件，去除首行标题，分割为列表，添加数据时间
    """
    def __init__(self):
        pass

    def conduct_cert(self, data):
        data[6] = data[6][:20]
        return data

    def read_file(self, path, file_time):
        file = open(path, 'r', encoding='utf-8')
        reader = csv.reader(file)
        n = 0
        for row in reader:
            # print(row)
            n += 1
            if n > 1:
                row = row[0].split('&#@')
                if row[0]:
                    if 'cert' in path:
                        row = self.conduct_cert(row)
                    row.append(file_time)

                    yield row



def main6(path):
    """"不导入交易数据"""
    file_lists = os.listdir(path)
    readfile = ReadFile()
    threads = []
    for file_name in file_lists:
        print('{}'.format(file_name))
        file_path = os.path.join(path, file_name)
        if file_path[-3:] == 'csv' and file_name[:4] != 'stif':
            parm_name = file_name.split('_')
            table_name = "load_" + parm_name[0]
            file_time = parm_name[1][:-4]
            datas = readfile.read_file(file_path, file_time)
            print('{}, 准备写入{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), file_name))

            connect = Save_MySQL()
            # connect.save(table_name, eval(table_name), datas)
            t = threading.Thread(target=connect.save2, args=(table_name, eval(table_name), datas))
            t.start()
            print('{}, 开始多线程写入{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), file_name))
            threads.append(t)

    for tr in threads:
        tr.join()
    print('{}, 全部线程已启动'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))


def main7(path):
    """单独处理交易数据"""
    file_lists = os.listdir(path)
    readfile = ReadFile()
    threads = []
    for file_name in file_lists:
        log.debug(file_name)
        file_path = os.path.join(path, file_name)
        if file_path[-3:] == 'csv' and file_name[:4] == 'stif':
            parm_name = file_name.split('_')
            table_name = "load_" + parm_name[0]
            file_time = parm_name[1][:-4]
            datas = readfile.read_file(file_path, file_time)
            log.info('准备写入{}'.format(file_name))
            print('{}, 准备写入{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), file_name))

            connect = Save_MySQL()
            # connect.save(table_name, eval(table_name), datas)
            t = threading.Thread(target=connect.save2, args=(table_name, eval(table_name), datas))
            t.start()
            log.info('开始多线程写入{}'.format(file_name))
            print('{}, 开始多线程写入{}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), file_name))
            threads.append(t)

    for tr in threads:
        tr.join()


def run_date(date):
    connect = Save_MySQL()
    times = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    connect.save4((date, times))


def just_make_org(num, stiftime, file_time, stif_num, tosql=None):
    org = Org(tosql)
    t_stan_org, org_contect = org.make_stan_org(num)
    common = CommnTable(t_stan_org, org.ctif_tp, tosql)
    t_stan_stif, stif_contect = common.make_stan_stif(stiftime)
    if org.tosql:
        org_contect.append(file_time)
    if org.tosql:
        stif_contect.append(file_time)
    # return (t_stan_org, t_stan_cert, t_stan_address, t_stan_tel, t_stan_relation, t_stan_pact, t_stan_bact, stifs)
    return (org_contect, stif_contect)


def just_make_person(num, stiftime, file_time, stif_num, tosql=None):
    person = Person(tosql)
    t_stan_person, person_contect = person.make_stan_person(num)
    return person_contect



def main8(begin, end, stiftime, file_time, stif_num):
    """
    生成数据，写入文件
    :param begin:
    :param end:
    :param stiftime: 交易日期
    :param file_time: 写入文件名的日期
    :param stif_num: 每个客户交易数量
    :return:
    """
    temp_person = []
    temp_org = []
    temp_stif = []
    for num in range(begin, end):
        # persons = just_make_person(num, stiftime, file_time, stif_num)
        # temp_person.append(persons)
        orgs, stifs = just_make_org(num, stiftime, file_time, stif_num)

        temp_org.append(orgs)
        temp_stif.append(stifs)

        parms = ["load_org","load_stif"]
        parm2 = [temp_org, temp_stif]
        name = ["t_stan_org", "t_stan_stif"]
        if num % 100000 == 0 and len(temp_org) != 0:
        # if num % 100 == 0 and len(temp_org) != 0:
            threads = []
            for inde, elem in enumerate(parm2):
                log.info('开始写入{}, 编号num={}, 数量={}'.format(parms[inde], num, len(temp_org)))
                print('{}, 开始写入{}, 编号num={}, 数量={}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
                                                           parms[inde], num, len(temp_org)))
                file_name = parms[inde].split('_')[-1] + "_" + file_time + '.csv'
                t = threading.Thread(target=write_to_csv_more, args=(file_name, elem))
                t.start()
                threads.append(t)

            for tr in threads:
                tr.join()

            for li in parm2:
                log.info('开始清理数据……')
                print('开始清理数据……')
                li.clear()



def main9(begin, end, stiftime, file_time, stif_num):

    """
    生成数据，写入mysql
    :param begin:
    :param end:
    :param stiftime: 交易日期
    :param file_time: 写入文件名的日期
    :param stif_num: 每个客户交易数量
    :return:
    """
    temp_person = []
    temp_org = []
    temp_relation = []
    temp_stifs = []

    for num in range(begin, end):
        # persons, cert, address, tel, relation, pact, bact, stifs = make_person(num, stiftime, file_time, stif_num, tosql=1)
        # temp_person.append(persons)
        # temp_cert.append(cert)
        # temp_address.append(address)
        # temp_tel.append(tel)
        # temp_pact.append(pact)
        # temp_bact.append(bact)
        # temp_relation.append(relation)
        # temp_stifs.extend(stifs)
        orgs, stifs2 = just_make_org(num, stiftime, file_time, stif_num, tosql=1)

        temp_org.append(orgs)
        # temp_cert.append(cert2)
        # temp_address.append(address2)
        # temp_tel.append(tel2)
        # temp_pact.append(pact2)
        # temp_bact.append(bact2)
        # temp_relation.append(relation2)
        temp_stifs.append(stifs2)

        parms = ["load_org", "load_stif"]
        parm2 = [temp_org, temp_stifs]

        if num % 10000 == 0 and len(temp_org) != 0:
            threads = []
            for inde, elem in enumerate(parm2):
                log.info('开始写入{}, 编号num={}, 数量={}'.format(parms[inde], num, len(temp_org)))
                print('{}, 开始写入{}, 编号num={}, 数量={}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), parms[inde], num, len(temp_org)))
                connect = Save_MySQL()
                t = threading.Thread(target=connect.save, args=(parms[inde], eval(parms[inde]), elem))
                t.start()
                threads.append(t)

            for tr in threads:
                tr.join()

            for li in parm2:
                log.info('开始清理数据……')
                print('开始清理数据……')
                li.clear()