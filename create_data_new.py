# -*- coding: utf-8 -*-
# @Time    : 2020/5/8 9:55
# @Author  : liudongyang
# @FileName: create_data_new.py
# @Software: PyCharm
# 重构，功能和业务拆分
import os
import time

from Common import *
from save_to_mysql import Save_MySQL
from loggers import LogInfo

loginfo = LogInfo()
log = loginfo.logger("DEBUG")


class CommnTable:
    def __init__(self, infos, ctif_tp, tosql=None):
        self.infos = infos
        # 主体表
        self.ctif_id = infos.get("busi_reg_no")  # 客户号
        self.ctif_tp = ctif_tp  # 主体类型
        self.client_tp = infos.get("client_tp")
        self.busi_type = infos.get('busi_type')
        self.citp = infos.get("citp")  # 证件类型
        self.citp_ori = infos.get("citp_ori")  # 证件类型原值
        self.citp_nt = infos.get("citp_nt")  # 证件类型说明
        self.ctid = infos.get("ctid")  # 证件号码
        self.mer_unit = infos.get("mer_unit")
        self.smid = infos.get("smid")
        self.ctnm = infos.get("ctnm")
        self.ctid = infos.get("ctid")
        self.tel = infos.get('tel')  # 电话
        # cert表
        self.ctid_edt = infos.get("ctid_edt")
        self.iss_ctry = infos.get("country")
        # address表
        self.address = infos.get("address")  # 详细地址==证件表证件地址
        # pact表
        self.act_cd = ''  # 支付账户号
        self.is_self_acc = ''  # 是否特约商户收单结算账号
        # bact表
        self.act_cd = ''
        self.cbac = ''
        self.cabm = ''
        self.act_tp = ''

        self.tosql = tosql  # 是否写入mysql

    # 客户证件表
    def make_stan_cert(self):
        """
        ctif_id:客户号
        ctif_tp:主体类型
        citp:证件类型
        citp_ori:证件类型原值
        citp_nt:证件类型说明
        ctid:证件号码
        iss_unt:证件签发机关
        address:证件地址
        ctid_edt:主体证件有效期
        iss_dt:证件签发日期
        iss_ctry:证件签发国家
        is_rp:是否主证件

        :return:
        """
        if len(self.ctid) > 15:
            iss_unt = make_province_city_process_data(self.ctid[:6])[:14] + "公安局"  # 取值户籍所在地县级公安局
        else:
            iss_unt = make_province_city_process_data(make_province_code_data())[:14] + "公安局"  # 取值户籍所在地县级公安局
        # iss_unt = make_province_code_data() + "公安局"  # 取值户籍所在地县级公安局
        iss_dt = make_iss_dt_data(self.ctid_edt)
        is_rp = "1"  # 考虑添加副证件
        if not self.tosql:
            contect_data = make_connect_data([
                self.ctif_id, self.ctif_tp, self.citp, self.citp_ori, self.citp_nt, self.ctid, iss_unt, self.address,
                self.ctid_edt, iss_dt, self.iss_ctry, is_rp
            ])
        else:
            contect_data = [self.ctif_id, self.ctif_tp, self.citp, self.citp_ori, self.citp_nt, self.ctid, iss_unt, self.address, self.ctid_edt, iss_dt, self.iss_ctry, is_rp]

        return {
                   "ctif_id": self.ctif_id,
                   "ctif_tp": self.ctif_tp,
                   "citp": self.citp,
                   "citp_ori": self.citp_ori,
                   "citp_nt": self.citp_nt,
                   "ctid": self.ctid,
                   "iss_unt": iss_unt,
                   "address": self.address,
                   "ctid_edt": self.ctid_edt,
                   "iss_dt": iss_dt,
                   "iss_ctry": self.iss_ctry,
                   "is_rp": is_rp
               }, contect_data

    # 客户地址信息表
    def make_stan_address(self):
        """
        ctif_id: 客户号
        ctif_tp: 主体类型
        address_tp: 地址类型
        address: 详细地址
        ctry: 国家代码
        county: 行政区划代码
        prvc: 省
        city: 市
        area: 区县
        postcode: 邮编
        exp_dt: 地址的失效日
        is_rp: 是否主地址
        :return:
        """
        ctif_id = self.ctif_id
        ctif_tp = self.ctif_tp  # 取值
        address_tp = make_address_tp_data()
        ctry = self.iss_ctry  # 取值
        # county = make_make_province_city_process_data(self.infos.get("ctid")[:6])  # 已从最新接口文档中移除
        prvc = self.infos.get("mer_prov")  # 取值
        city = self.infos.get("mer_city")  # 取值
        area = self.infos.get("mer_area")  # 取值
        postcode = ""
        exp_dt = ""
        is_rp = "1"
        if not self.tosql:
            contect_data = make_connect_data([ctif_id, ctif_tp, address_tp, self.address, ctry, prvc, city, area, postcode, exp_dt, is_rp])
        else:
            contect_data = [ctif_id, ctif_tp, address_tp, self.address, ctry, prvc, city, area, postcode, exp_dt, is_rp]
        return {
                   "ctif_id": ctif_id,
                   "ctif_tp": ctif_tp,
                   "address_tp": address_tp,
                   "address": self.address,
                   "ctry": ctry,
                   "prvc": prvc,
                   "city": city,
                   "area": area,
                   "postcode": postcode,
                   "exp_dt": exp_dt,
                   "is_rp": is_rp
               }, contect_data

    # 客户联系信息表
    def make_stan_tel(self):
        """
        ctif_id:客户号
        ctif_tp:主体类型
        tel_tp:电话类型
        tel:联系电话
        is_rp:是否主电话
        :return:
        """
        tel_tp = random.choice(["11", "12", "22"])
        is_rp = "1"
        if not self.tosql:
            contect_data = make_connect_data([
                self.ctif_id, self.ctif_tp, tel_tp, self.tel, is_rp
            ])
        else:
            contect_data = [self.ctif_id, self.ctif_tp, tel_tp, self.tel, is_rp]

        return {
                   "ctif_id": self.ctif_id,
                   "ctif_tp": self.ctif_tp,
                   "tel_tp": tel_tp,
                   "tel": self.tel,
                   "is_rp": is_rp
               }, contect_data

    # 关系人信息表
    def make_stan_relation(self):
        """
        客户关系
        ctif_id: 客户号
        ctif_tp: 主体类型
        rel_tp: 关系类型
        rel_layer: 关系人层级
        rel_ctif: 关系人客户号
        rel_cstp: 关系人类别
        rel_name: 关系人名称
        rcnt: 关系人国籍/国家
        citp: 关系人证件类型
        citp_ori: 关系人证件类型原
        ctid: 关系人证件号码
        citp_nt: 关系人证件类型说
        hold_per: 持股比例
        hold_amt: 持股金额
        ctid_edt: 关系人证件有效期
        rel_prov: 关系人省
        rel_city: 关系人市
        rel_area: 关系人区县
        rear: 关系人详细地址
        retl: 关系人联系电话
        :return:
        """
        rel_tp = make_rel_tp_data(self.ctif_tp)
        rel_layer = random.choice(["-1", "0", "1", "2", "3", "4", "5"])
        # rel_ctif = make_random_num(6)
        rel_ctif = ''
        # rel_cstp = random.choice(["1", "2"])
        rel_cstp = '2'
        rel_name = make_name_data()
        rcnt = "CHN"  # make_country_data()  默认中国
        citp = make_citp_data()
        citp_ori = citp
        ctid = make_ctid_data()
        citp_nt = "证件类型说明"
        hold_per = 0.05  # 持股比例
        hold_amt = 0.05  # 持股金额
        ctid_edt = make_Card_valid_date(ctid)
        rel_prov = get_province_code_data(ctid[:6])
        rel_city = make_province_city_code_data(ctid[:6])
        rel_area = ctid[:6]
        rear = make_address(ctid[:6])
        retl = make_tel_num()
        if not self.tosql:
            contect_data = make_connect_data([
                self.ctif_id, self.ctif_tp, rel_tp, rel_layer, rel_ctif, rel_cstp, rel_name, rcnt, citp, citp_ori, ctid,
                citp_nt, hold_per, hold_amt, ctid_edt, rel_prov, rel_city, rel_area, rear, retl])
        else:
            contect_data = [self.ctif_id, self.ctif_tp, rel_tp, rel_layer, rel_ctif, rel_cstp, rel_name, rcnt, citp,
                            citp_ori, ctid,citp_nt, hold_per, hold_amt, ctid_edt, rel_prov, rel_city, rel_area, rear,
                            retl]
        return {
                   "ctif_id": self.ctif_id,
                   "ctif_tp": self.ctif_tp,
                   "rel_tp": rel_tp,
                   "rel_layer": rel_layer,
                   "rel_ctif": rel_ctif,
                   "rel_cstp": rel_cstp,
                   "rel_name": rel_name,
                   "rcnt": rcnt,
                   "citp": citp,
                   "citp_ori": citp_ori,
                   "ctid": ctid,
                   "citp_nt": citp_nt,
                   "hold_per": hold_per,
                   "hold_amt": hold_amt,
                   "ctid_edt": ctid_edt,
                   "rel_prov": rel_prov,
                   "rel_city": rel_city,
                   "rel_area": rel_area,
                   "rear": rear,
                   "retl": retl
               }, contect_data

    # 支付账户表
    def make_stan_pact(self):
        """
        ctif_id: 客户号
        ctif_tp: 主体类型
        act_tp: 账户类型
        act_cd: 支付账户号
        act_typ: 账号类别
        act_limit: 支付账户交易限额
        is_self_acc: 是否特约商户收单结算账号
        sales_name: 预付卡办卡人
        cst_sex: 预付卡办卡人性别
        nation: 预付卡办卡人国籍
        occupation: 预付卡办卡人职业
        id_type: 预付卡办卡人证件种类
        id_type_ori: 预付卡办卡人证件种类原值
        id_no: 预付卡办卡人证件号码
        id_deadline: 预付卡办卡人证件有效期截至日
        contact: 预付卡办卡人联系方式
        address: 预付卡办卡人住所地或工作单位地址
        sales_flag: 预付卡代直销标识
        bind_mob: 绑定的手机号码
        mer_unit: 管理机构
        cls_dt: 账户状态
        rgdt: 开户日期
        cls_stat: 销户日期

        :return:
        """
        if self.ctif_id == "02":
            return '', ''

        else:
            act_tp = random.choice(['11', "211", "212"])
            self.act_cd = make_act_cd_data(act_tp)
            act_typ = make_act_type_data(act_tp)
            act_limit = make_act_limit_data(act_tp, act_typ)
            self.is_self_acc = random.choice(["0", "1"])
            if self.busi_type == "03":
                sales_name = self.ctnm
                cst_sex = str(random.randint(0, 3))
                nation = self.iss_ctry
                occupation = '职业'
                id_type = self.citp
                address = self.address
                id_type_ori = self.citp_ori
                id_no = self.ctid
                id_deadline = self.ctid_edt
                contact = self.tel
                sales_flag = random.choice(["11", "12"])
            else:
                sales_name, cst_sex, nation, occupation, id_type, id_type_ori, id_no, id_deadline, contact, address, sales_flag = [
                    '' for i in range(11)]
            if self.busi_type == '01':
                bind_mob = self.tel
            else:
                bind_mob = ''
            cls_dt = make_cls_dt_data(self.ctif_id)
            rgdt = make_register_date()
            if cls_dt == "C":
                cls_stat = make_register_date()
            else:
                cls_stat = ""
            if not self.tosql:
                contect_data = make_connect_data([
                    self.ctif_id, self.ctif_tp, act_tp, self.act_cd, act_typ, act_limit, self.is_self_acc, sales_name,
                    cst_sex,
                    nation,
                    occupation,
                    id_type, id_type_ori, id_no, id_deadline, contact, address, sales_flag, bind_mob, self.mer_unit, cls_dt,
                    rgdt,
                    cls_stat
                ])
            else:
                contect_data = [self.ctif_id, self.ctif_tp, act_tp, self.act_cd, act_typ, act_limit, self.is_self_acc,
                                sales_name, cst_sex, nation, occupation, id_type, id_type_ori, id_no, id_deadline,
                                contact, address, sales_flag, bind_mob, self.mer_unit, cls_dt, rgdt, cls_stat]

            return {
                       "ctif_id": self.ctif_id,
                       "ctif_tp": self.ctif_tp,
                       "act_tp": act_tp,
                       "act_cd": self.act_cd,
                       "act_typ": act_typ,
                       "act_limit": act_limit,
                       "is_self_acc": self.is_self_acc,
                       "sales_name": sales_name,
                       "cst_sex": cst_sex,
                       "nation": nation,
                       "occupation": occupation,
                       "id_type": id_type,
                       "id_type_ori": id_type_ori,
                       "id_no": id_no,
                       "id_deadline": id_deadline,
                       "contact": contact,
                       "address": address,
                       "sales_flag": sales_flag,
                       "bind_mob": bind_mob,
                       "mer_unit": self.mer_unit,
                       "cls_dt": cls_dt,
                       "rgdt": rgdt,
                       "cls_stat": cls_stat
                   }, contect_data

    # 银行账户表
    def make_stan_bact(self):
        """
        ctif_id: 客户号
        ctif_tp: 主体类型
        act_tp: 银行账号种类
        act_flag: 银行账号种类-现场检查
        act_cd: 银行账户号
        cabm: 银行账号开户行名称
        pay_id: 关联支付账户
        is_self_acc: 是否特约商户收单结算账号
        bank_acc_name: 银行账户名称
        mer_unit: 管理机构
        :return:
        """
        self.act_tp = make_bank_act_tp_data(self.ctif_tp)
        act_flag = random.choice(["11", "12"])
        act_cd = "62" + make_random_num(17)
        if self.ctif_tp == '2':
            self.cabm = make_cabm_data(make_province_code_data())
        is_self_acc = self.is_self_acc
        mer_unit = self.mer_unit
        pay_id = self.act_cd
        bank_acc_name = self.infos.get('ctnm')  # 没明白是什么，暂空
        if not self.tosql:
            contect_data = make_connect_data([self.ctif_id, self.ctif_tp, self.act_tp, act_flag, act_cd, self.cabm,
                                              pay_id, is_self_acc, bank_acc_name, mer_unit])
        else:
            contect_data = [self.ctif_id, self.ctif_tp, self.act_tp, act_flag, act_cd, self.cabm, pay_id, is_self_acc,
                            bank_acc_name,mer_unit]
        return {
                   "ctif_id": self.ctif_id,
                   "ctif_tp": self.ctif_tp,
                   "act_tp": self.act_tp,
                   "act_flag": act_flag,
                   "act_cd": act_cd,
                   "cabm": self.cabm,
                   "pay_id": pay_id,
                   "is_self_acc": is_self_acc,
                   "bank_acc_name": bank_acc_name,
                   "mer_unit": mer_unit
               }, contect_data

    # 标准交易表
    def make_stan_stif(self, stif_time):
        """
        ctif_id: 主体客户号
        ctif_tp: 主体类别
        client_tp: 客户类别
        smid: 主体特约商户编码
        ctnm: 主体姓名/名称
        citp: 主体身份证件/证明文件类型
        citp_ori: 主体身份证件/证明文件类型原值
        citp_nt: 主体身份证件/证明文件类型说明
        ctid: 主体身份证件/证明文件号码
        cbat: 主体的银行账号种类
        cbac: 主体的银行账号
        cabm: 主体银行账号的开户行名称
        ctat: 主体的交易账号种类
        ctac: 主体的交易账号
        cpin: 主体所在支付机构的名称
        cpba: 主体所在支付机构的银行账号
        cpbn: 主体所在支付机构的银行账号的开户行名称
        ctip: 主体的交易IP地址
        tstm: 交易时间
        cttp: 货币资金转移方式
        tsdr: 资金收付标志
        crpp: 资金用途
        crtp: 交易币种
        crat: 交易金额
        tcif_id: 交易对手ID
        tcnm: 交易对手姓名/名称
        tsmi: 交易对手特约商户编码
        tcit: 交易对手证件/证明文件类型
        tcit_ori: 交易对手证件/证明文件类型原值
        tcit_nt: 交易对手证件/证明文件类型说明
        tcid: 交易对手证件/证明文件号码
        tcat: 交易对手的银行账号种类
        tcba: 交易对手的银行账号
        tcbn: 交易对手银行账号的开户行名称
        tctt: 交易对手的交易账号种类
        tcta: 交易对手的交易账号
        tcpn: 交易对手所在支付机构的名称
        tcpa: 交易对手所在支付机构的银行账号
        tpbn: 交易对手所在支付机构银行账号的开户行名称
        tcip: 交易对手的交易IP地址
        tmnm: 交易商品名称
        bptc: 银行与支付机构之间的业务交易编码
        pmtc: 支付机构与商户之间的业务交易编码
        ticd: 业务标识号
        busi_type: 业务类型
        trans_type: 交易类型
        pos_dev_id: 交易终端号或IMEI号等设备标识
        trans_stat: 交易状态
        bank_stat: 银行状态
        mer_prov: 地区省
        mer_area: 地区县
        pos_prov: 交易省
        pos_area: 交易县
        mer_unit: 管理机构
        extend1: 转换标识
        iofg: 境内外标识
        trans_channel: 交易渠道
        ctmac: 交易发生的mac地址
        balance: 主体支付账户的余额
        acc_flag: 交易对方账户类型
        ctid_edt: 主体身份证件/证明文件有效期截止日
        tran_flag: 对手账号标识
        trans_order: 交易订单号
        trans_cst_type: 交易类型(客户定义)
        crat_u: 交易金额折合美元
        crat_c: 交易金额折合人民币
        trans_way: 交易方式
        agency_ctnm: 代办人姓名
        agency_citp: 代办人身份证件（证明文件）类型
        agency_ctid: 代办人身份证件（证明文件）号码
        agency_country: 代办人国籍
        :return:
        """
        busi_type = make_busi_type_data()  #
        cbat = self.act_tp
        ctat = make_ctat_data(busi_type)
        ctac = make_random_num(17)
        cpin = "机构名称"
        cpba = make_random_num(17)
        cpbn = make_cabm_data(make_province_code_data())
        ctip = make_ip_data(busi_type)
        # tstm = make_trade_time_data()
        tstm = stif_time
        cttp = make_cttp_data()
        tsdr = random.choice(["01", "02"])
        crpp = "资金用途"
        crtp = "CNY"
        crat = make_crat_data()
        tcif_id = make_tcif_id_data(busi_type)
        tsmi = make_random_num(20)
        tcit = make_cert_type_data()
        if tcit == '21' or '22':
            tcnm = make_name_data(4)
        else:
            tcnm = make_name_data()

        tcit_ori = tcit
        tcit_nt = "证件类型说明"
        if tcit == "11":
            tcid = make_ctid_data()
        elif tcit == '21':
            tcid = make_random_num(9)
        else:
            tcid = make_random_num(20)
        tcat = random.choice(["01", "02", "03"])
        tcba = make_random_num(19)
        tcbn = make_cabm_data(make_province_code_data())
        tctt = random.choice(["01", "02"])
        tcta = make_random_num(19)
        tcpn = "支付机构名称"
        tcpa = make_random_num(19)
        tpbn = make_cabm_data(make_province_code_data())
        tcip = make_ip_data(busi_type)
        tmnm = "商品名称"
        bptc = make_random_num(25)
        pmtc = make_random_num(25)
        ticd = make_ticd_data()
        trans_type = make_trans_type_data(busi_type)
        pos_dev_id = make_pos_dev_id_data(busi_type)
        trans_stat = "S"  # 交易状态，需提供支付系统码表
        bank_stat = "bank_stat"  # 银行状态，需提供支付系统码表
        province_code = make_province_code_data()
        mer_prov = get_province_data(province_code)
        mer_area = province_code

        province_code2 = make_province_code_data()
        pos_prov = get_province_data(province_code2)
        pos_area = province_code2

        extend1 = ""
        # rate_rmb = ""   # 老接口字段
        # rate_usa = ""  # 老接口字段
        iofg = "0"  # 暂时默认境内交易
        trans_channel = make_trans_channel_data()
        ctmac = make_mac_info_data(busi_type)
        balance = "10000"
        acc_flag = make_acc_flag_data(busi_type)
        tran_flag = make_tran_flag_data(busi_type)

        trans_order = make_trans_order_data(busi_type)
        trans_cst_type = make_trans_cst_type_data()
        crat_u = make_crat_u_data(crat)
        crat_c = make_crat_r_data(crat)
        trans_way = '000030'  # make_random_str(6)  # 详见交易方式代码表(目前未收到人行的接口文件，暂定6位)
        agency_ctnm = make_name_data()
        agency_citp = make_citp_data()
        agency_ctid = make_ctid_data()
        agency_country = "CHN"
        if not self.tosql:
            contect_data = make_connect_data([
                self.ctif_id, self.ctif_tp, self.client_tp, self.smid, self.ctnm, self.citp, self.citp_ori, self.citp_nt,
                self.ctid, cbat, self.cbac, self.cabm, ctat, ctac, cpin,
                cpba, cpbn, ctip, tstm, cttp, tsdr, crpp, crtp, crat, tcif_id, tcnm, tsmi, tcit, tcit_ori, tcit_nt, tcid,
                tcat,
                tcba, tcbn, tctt, tcta, tcpn, tcpa, tpbn, tcip, tmnm, bptc, pmtc, ticd, busi_type, trans_type, pos_dev_id,
                trans_stat, bank_stat, mer_prov, mer_area, pos_prov, pos_area, self.mer_unit, extend1, iofg, trans_channel,
                ctmac,
                balance, acc_flag, self.ctid_edt, tran_flag, trans_order, trans_cst_type, crat_u, crat_c, trans_way,
                agency_ctnm,
                agency_citp, agency_ctid, agency_country
            ])
        else:
            contect_data = [
                self.ctif_id, self.ctif_tp, self.client_tp, self.smid, self.ctnm, self.citp, self.citp_ori, self.citp_nt,
                self.ctid, cbat, self.cbac, self.cabm, ctat, ctac, cpin,
                cpba, cpbn, ctip, tstm, cttp, tsdr, crpp, crtp, crat, tcif_id, tcnm, tsmi, tcit, tcit_ori, tcit_nt, tcid,
                tcat,
                tcba, tcbn, tctt, tcta, tcpn, tcpa, tpbn, tcip, tmnm, bptc, pmtc, ticd, busi_type, trans_type, pos_dev_id,
                trans_stat, bank_stat, mer_prov, mer_area, pos_prov, pos_area, self.mer_unit, extend1, iofg, trans_channel,
                ctmac,
                balance, acc_flag, self.ctid_edt, tran_flag, trans_order, trans_cst_type, crat_u, crat_c, trans_way,
                agency_ctnm,
                agency_citp, agency_ctid, agency_country
            ]
        return {
                   "ctif_id": self.ctif_id,
                   "ctif_tp": self.ctif_tp,
                   "client_tp": self.client_tp,
                   "smid": self.smid,
                   "ctnm": self.ctnm,
                   "citp": self.citp,
                   "citp_ori": self.citp_ori,
                   "citp_nt": self.citp_nt,
                   "ctid": self.ctid,
                   "cbat": cbat,
                   "cbac": self.cbac,
                   "cabm": self.cabm,
                   "ctat": ctat,
                   "ctac": ctac,
                   "cpin": cpin,
                   "cpba": cpba,
                   "cpbn": cpbn,
                   "ctip": ctip,
                   "tstm": tstm,
                   "cttp": cttp,
                   "tsdr": tsdr,
                   "crpp": crpp,
                   "crtp": crtp,
                   "crat": crat,
                   "tcif_id": tcif_id,
                   "tcnm": tcnm,
                   "tsmi": tsmi,
                   "tcit": tcit,
                   "tcit_ori": tcit_ori,
                   "tcit_nt": tcit_nt,
                   "tcid": tcid,
                   "tcat": tcat,
                   "tcba": tcba,
                   "tcbn": tcbn,
                   "tctt": tctt,
                   "tcta": tcta,
                   "tcpn": tcpn,
                   "tcpa": tcpa,
                   "tpbn": tpbn,
                   "tcip": tcip,
                   "tmnm": tmnm,
                   "bptc": bptc,
                   "pmtc": pmtc,
                   "ticd": ticd,
                   "busi_type": busi_type,
                   "trans_type": trans_type,
                   "pos_dev_id": pos_dev_id,
                   "trans_stat": trans_stat,
                   "bank_stat": bank_stat,
                   "mer_prov": mer_prov,
                   "mer_area": mer_area,
                   "pos_prov": pos_prov,
                   "pos_area": pos_area,
                   "mer_unit": self.mer_unit,
                   "extend1": extend1,
                   "iofg": iofg,
                   "trans_channel": trans_channel,
                   "ctmac": ctmac,
                   "balance": balance,
                   "acc_flag": acc_flag,
                   "ctid_edt": self.ctid_edt,
                   "tran_flag": tran_flag,
                   "trans_order": trans_order,
                   "trans_cst_type": trans_cst_type,
                   "crat_u": crat_u,
                   "crat_c": crat_c,
                   "trans_way": trans_way,
                   "agency_ctnm": agency_ctnm,
                   "agency_citp": agency_citp,
                   "agency_ctid": agency_ctid,
                   "agency_country": agency_country
               }, contect_data

        # 生成个人表


class Person:
    def __init__(self, tosql=None):
        self.ctif_tp = '1'
        self.tosql = tosql

    def make_stan_person(self, num, client_type=None):
        """字段列表
        "busi_reg_no":"客户号",
        "ctnm":"客户名称",
        "cten":"拼音/英文名称",
        "client_tp":"客户类别",1客户，2商户
        "account_tp":"账户分类",1/2/3代表1、2、3类账号
        "busi_type":"业务类型",
        "smid":"主体特约商户编号",
        "citp":"证件类型",
        "citp_ori":"证件类型原值",
        "citp_nt":"证件类型说明",
        "ctid":"证件号码",
        "ctid_edt":"证件有效期",
        "sex":"性别",
        "country":"国籍",
        "nation":"民族",
        "birthday":"出生日期",
        "education":"学历",
        "ctvc":"主体的职业类别",
        "picm":"个人年收入",
        "ficm":"家庭年收入",
        "marriage":"婚姻状况",
        "ceml":"电子邮件",
        "rgdt":"开户日期",
        "cls_dt":"销户日期",
        "remark":"备注",
        "indu_code":"行业代码",
        "stat_flag_ori":"客户状态原值",
        "stat_flag":"客户状态",
        "mer_prov":"省",
        "mer_city":"市",
        "mer_area":"区县",
        "address":"详细地址",
        "tel":"联系电话",
        "mer_unit":"管理机构",
        "is_line":"是否线上{注册}",
        "certification ":"建立渠道",
        "cer_num":"通过身份验证渠道数量",
        "con_acc_name":"经营名称",
        "bord_flag":"境内外标识",
        "web_info":"网络支付商户网址信息",
        "con_nation":"商户所属国家或地区",
        "bind_card":"银行绑定标识",
        "ip_code":"注册地IP地址",
        "mac_info":"注册设备MAC或IMEI地址",
        "self_acc_no":"特约商户收单结算账号",
        "acc_type1":"账户类型",
        "bank_acc_name":"银行账户名称",
        "reals":"客户真实有效性",
        "batch_pay":"批量代付标识",
        "statement_type":"结算类型"
        :return:
        """

        ctnm = make_name_data()
        cten = word_to_pinyin(ctnm)

        if client_type and client_type != 3:
            client_tp = client_type
        else:
            client_tp = random.choice(["1", "2"])

        if not client_type or client_type == 3:
            busi_reg_no = "p6_{}_{}".format(client_tp, num)
        else:
            busi_reg_no = num

        busi_type = make_busi_type_data()
        account_tp = make_account_tp_data(busi_type)
        if client_tp == "2":
            smid = random.randint(1000, 9999)  # 该字段值待确定
            smid = str(smid)
        else:
            smid = ""
        citp = make_citp_data()
        citp_ori = citp  # 该值暂定
        citp_nt = "有效证件"
        ctid = make_ctid_data()
        ctid_edt = make_Card_valid_date(ctid)
        sex = make_sex(ctid)
        country = choice_contry()
        nation = str(random.randint(1, 57))
        birthday = ctid[6:14]
        education = str(random.randint(1, 7))
        ctvc = random.choice(["1A", "1B", "1C", "1D", "1E", "1F", "1G", "1H"])
        picm = "300000"
        ficm = "500000"
        marriage = make_marriage_data(ctid)
        ceml = make_email_data()
        rgdt = make_register_date()
        cls_dt = make_cls_dt_data(busi_reg_no)
        remark = "这是一个备注"
        indu_code = make_indu_code_data()
        stat_flag_ori = make_stat_flag_data(cls_dt)
        stat_flag = stat_flag_ori
        # mer_prov = get_province_data(ctid[:6])
        mer_prov = get_province_code_data(ctid[:6])
        # mer_city = make_province_city_data(ctid[:6])[0]
        mer_city = make_province_city_code_data(ctid[:6])
        # mer_area = make_province_city_data(ctid[:6])[-1]
        mer_area = ctid[:6]
        address = make_address(ctid[:6])
        tel = make_tel_num()
        mer_unit = make_mer_unit_data()
        is_line = random.choice(["0", "1"])
        certification = random.choice(["1", "2", "3"])
        cer_num = str(random.randint(0, 6))
        con_acc_name = "默认经营名称"  # 网络支付、预付卡、银行卡收单必须填写,暂为空
        bord_flag = make_bord_flag_data()
        web_info = make_web_info_data(busi_type)  # 非网络支付业务，无网址用户可不填
        con_nation = make_con_nation_data(bord_flag)
        bind_card = make_bind_card_data(busi_type)  # 仅需网络支付填写
        ip_code = make_ip_data(busi_type)  # 仅需网络支付填写
        mac_info = make_mac_info_data(busi_type)  # PC机填写MAC，移动终端填写IMEI(需网络支付，预付卡填写), 暂为空
        self_acc_no = make_self_acc_no_data(client_tp)  # 非商户不填，网络支付、预付卡、银行卡收单必须填写
        acc_type1 = make_acc_type1_data(client_tp)  # 非商户不填，网络支付、预付卡、银行卡收单必须填写
        bank_acc_name = make_bank_acc_name_data(acc_type1)
        reals = make_reals_data()
        batch_pay = make_batch_pay_data(busi_type, client_tp)
        statement_type = make_statement_type_data(client_tp)
        if not self.tosql:
            contect_data = make_connect_data([
            busi_reg_no, ctnm, cten, client_tp, account_tp, busi_type, smid, citp, citp_ori, citp_nt, ctid, ctid_edt,
            sex,
            country, nation, birthday, education, ctvc, picm, ficm, marriage, ceml, rgdt, cls_dt, remark, indu_code,
            stat_flag_ori, stat_flag, mer_prov, mer_city, mer_area, address, tel, mer_unit, is_line, certification,
            cer_num, con_acc_name, bord_flag, web_info, con_nation, bind_card, ip_code, mac_info, self_acc_no,
            acc_type1,
            bank_acc_name, reals, batch_pay, statement_type
        ])
        else:
            contect_data = [
            busi_reg_no, ctnm, cten, client_tp, account_tp, busi_type, smid, citp, citp_ori, citp_nt, ctid, ctid_edt,
            sex,
            country, nation, birthday, education, ctvc, picm, ficm, marriage, ceml, rgdt, cls_dt, remark, indu_code,
            stat_flag_ori, stat_flag, mer_prov, mer_city, mer_area, address, tel, mer_unit, is_line, certification,
            cer_num, con_acc_name, bord_flag, web_info, con_nation, bind_card, ip_code, mac_info, self_acc_no,
            acc_type1,
            bank_acc_name, reals, batch_pay, statement_type
        ]
        return {
                   "busi_reg_no": busi_reg_no,
                   "ctnm": ctnm,
                   "cten": cten,
                   "client_tp": client_tp,
                   "account_tp": account_tp,
                   "busi_type": busi_type,
                   "smid": smid,
                   "citp": citp,
                   "citp_ori": citp_ori,
                   "citp_nt": citp_nt,
                   "ctid": ctid,
                   "ctid_edt": ctid_edt,
                   "sex": sex,
                   "country": country,
                   "nation": nation,
                   "birthday": birthday,
                   "education": education,
                   "ctvc": ctvc,
                   "picm": picm,
                   "ficm": ficm,
                   "marriage": marriage,
                   "ceml": ceml,
                   "rgdt": rgdt,
                   "cls_dt": cls_dt,
                   "remark": remark,
                   "indu_code": indu_code,
                   "stat_flag_ori": stat_flag_ori,
                   "stat_flag": stat_flag,
                   "mer_prov": mer_prov,
                   "mer_city": mer_city,
                   "mer_area": mer_area,
                   "address": address,
                   "tel": tel,
                   "mer_unit": mer_unit,
                   "is_line": is_line,
                   "certification": certification,
                   "cer_num": cer_num,
                   "con_acc_name": con_acc_name,
                   "bord_flag": bord_flag,
                   "web_info": web_info,
                   "con_nation": con_nation,
                   "bind_card": bind_card,
                   "ip_code": ip_code,
                   "mac_info": mac_info,
                   "self_acc_no": self_acc_no,
                   "acc_type1": acc_type1,
                   "bank_acc_name": bank_acc_name,
                   "reals": reals,
                   "batch_pay": batch_pay,
                   "statement_type": statement_type
               }, contect_data


class Org:
    def __init__(self, tosql=None):
        self.ctif_tp = '2'
        self.tosql = tosql

    # 生成机构表
    def make_stan_org(self, num, client_type=None):
        """
        busi_reg_no: 客户号
        ctnm: 客户名称
        cten: 拼音/英文名称
        client_tp: 客户类别
        account_tp: 账户分类
        busi_type: 业务类型
        smid: 主体特约商户编号
        citp: 证件类型_报送
        citp_ori: 证件类型原值
        ctid: 证件号码
        ctid_edt: 证件有效期
        citp_nt: 证件类型说明
        id_type: 证件类型_现场检查
        org_no: 组织机构代码
        linkman: 联系人姓名
        linktel: 联系人手机号
        linkjob: 联系人职务
        linkmail: 联系人邮箱
        linkphone: 联系人固定电话
        ceml: 电子邮件
        ctvc: 主体的行业类别
        crnm: 主体的法定代表人姓名
        crit: 主体的法定代表人身份证件类型
        crit_ori: 主体的法定代表人身份证件类型原值
        crit_nt: 主体的法定代表人身份证件类型说明
        crid: 主体的法定代表人身份证件号码
        crid_edt: 主体的法定代表人证件有效期
        rgdt: 开户日期
        cls_dt: 销户日期
        scale: 企业规模
        country: 注册国家
        crp_type: 组织机构类别
        fud_date: 成立日期
        reg_cptl: 注册资本
        remark_ctvc: 经营范围
        agency_ctnm: 代办理人姓名
        agency_citp: 代办理人证件类型
        agency_ctid: 代办理人证件号码
        agency_edt: 代办理人证件有效期限
        remark: 备注
        indu_code: 行业代码
        stat_flag_ori: 客户状态原值
        stat_flag: 客户状态
        mer_prov: 省
        mer_city: 市
        mer_area: 区县
        address: 详细地址
        tel: 联系电话
        mer_unit: 管理机构
        is_line: 是否线上
        certification : 建立渠道
        cer_num: 通过身份验证渠道数量
        con_acc_name: 经营名称
        bord_flag: 境内外标识
        web_info: 网络支付商户网址信息
        con_nation: 商户所属国家或地区
        majority_shareholder_ctnm: 控股股东或实际控制人姓名
        majority_shareholder_citp: 控股股东或实际控制人证件类型
        majority_shareholder_citp_ori: 控股股东或实际控制人证件类型原值
        majority_shareholder_ctid: 控股股东或实际控制人证件号码
        majority_shareholder_edt: 控股股东或实际控制人证件有效期限
        reg_cptl_code: 注册资本金币种
        bind_card: 银行绑定标识
        ip_code: 注册地IP地址
        mac_info: 注册设备MAC或IMEI地址
        self_acc_no: 特约商户收单结算账号
        acc_type1: 账户类型
        bank_acc_name: 银行账户名称
        reals: 客户真实有效性
        complex: 非自然人结构复杂度
        clear: 非自然人股权可辨识度
        batch_pay: 批量代付标识
        statement_type: 结算类型
        :return:
        """
        ctnm = org_name_data()
        cten = word_to_pinyin(ctnm)

        if client_type and client_type != 3:
            client_tp = client_type
        else:
            client_tp = random.choice(["1", "2"])

        if not client_type or client_type == 3:
            busi_reg_no = "o6_{}_{}".format(client_tp, num)
        else:
            busi_reg_no = num

        busi_type = make_busi_type_data()
        account_tp = make_account_tp_data(busi_type)
        if client_tp == "2":
            smid = make_random_str(20)  # 该字段值待确定
        else:
            smid = ""
        citp = random.choice(["21", "29"])
        citp_ori = citp  # 该值暂定
        # ctid = make_ctid_data()
        ctid = make_random_num(9)
        # ctid = make_province_code_data() + make_random_num(3)
        # ctid_edt = make_Card_valid_date(ctid)
        ctid_edt = make_enable_date()  # 有效期
        if citp == "29":
            citp_nt = random.choice(["营业执照", "统一社会信用代码"])
        else:
            citp_nt = "证件类型"

        if citp_ori == "营业执照":
            id_type = "11"
        else:
            id_type = "12"

        org_no = make_random_num(9)  # 统一社会信用代码9-17位
        linkman = make_name_data()
        linktel = make_tel_num()
        linkjob = random.choice(['普通员工', '经理', '主管', '组长', '总监', '副总经理', '总经理'])
        linkmail = make_email_data()
        linkphone = make_random_num(9)
        ceml = make_email_data()
        ctvc = make_org_ctvc_data()
        crnm = make_name_data()
        crit = make_citp_data()
        crit_ori = crit
        if crit == "19":
            crit_nt = "证件类型说明"
        else:
            crit_nt = ""
        crid = make_ctid_data()
        crid_edt = make_Card_valid_date(crid)
        rgdt = make_register_date()
        cls_dt = make_cls_dt_data(busi_reg_no)
        scale = make_scale_data()
        country = make_country_data()
        crp_type = make_crp_type_data()
        fud_date = "20151111"  # 成立日期，暂时写死
        reg_cptl = "1000000.00"  # 注册资金，暂时写死
        remark_ctvc = "经营范围"
        agency_ctnm = make_name_data()
        agency_citp = make_citp_data()
        agency_ctid = make_ctid_data()
        agency_edt = make_Card_valid_date(agency_ctid)
        remark = "备注，暂时不填"
        indu_code = make_indu_code_data()  # 支付机构行业代码，
        stat_flag_ori = make_stat_flag_data(cls_dt)  # 客户状态原值，可是用支付系统码表，根据客户业务系统修改，默认正常为n,关闭为c
        stat_flag = stat_flag_ori  # 默认等于客户状态原值
        #  使用个人身份证
        # mer_prov = get_province_code_data(ctid[:6])
        # mer_city = make_province_city_code_data(ctid[:6])
        # mer_area = ctid[:6]
        # address = make_address(ctid[:6])

        mer_area = make_province_code_data()
        mer_prov = get_province_code_data(mer_area[:2] + "0000")
        mer_city = make_province_city_code_data(mer_area)
        address = make_address(mer_area)

        tel = make_tel_num()
        mer_unit = make_mer_unit_data()
        is_line = random.choice(["0", "1"])
        certification = random.choice(["1", "2", "3"])
        cer_num = str(random.randint(0, 6))
        con_acc_name = "默认经营名称"  # 网络支付、预付卡、银行卡收单必须填写,暂为空
        bord_flag = make_bord_flag_data()  # 网络支付、预付卡、银行卡收单必须填写
        web_info = make_web_info_data(busi_type)  # 非网络支付业务，无网址用户可不填
        con_nation = make_con_nation_data(bord_flag)  # 网络支付、预付卡、银行卡收单必须填写
        majority_shareholder_ctnm = make_name_data()
        majority_shareholder_citp = make_citp_data()
        majority_shareholder_citp_ori = majority_shareholder_citp
        majority_shareholder_ctid = make_ctid_data()
        majority_shareholder_edt = make_Card_valid_date(majority_shareholder_ctid)
        reg_cptl_code = "CNY"
        bind_card = make_bind_card_data(busi_type)  # 仅需网络支付填写
        ip_code = make_ip_data(busi_type)  # 仅需网络支付填写
        mac_info = make_mac_info_data(busi_type)  # PC机填写MAC，移动终端填写IMEI(需网络支付，预付卡填写), 暂为空
        self_acc_no = make_self_acc_no_data(client_tp)  # 非商户不填，网络支付、预付卡、银行卡收单必须填写
        acc_type1 = make_acc_type1_data(client_tp)  # 非商户不填，网络支付、预付卡、银行卡收单必须填写
        bank_acc_name = make_bank_acc_name_data(acc_type1)  # 当acc_type1=12时填写，银行账号对应账户名称（ 网络支付、预付卡、银行卡收单均需填写）
        reals = str(random.randint(1, 5))
        complex = make_complex_data()
        clear = make_clear_data()
        batch_pay = make_batch_pay_data(busi_type, client_tp)
        statement_type = random.choice(["0", "1"])
        if not self.tosql:
            contect_data = make_connect_data([
            busi_reg_no, ctnm, cten, client_tp, account_tp, busi_type, smid, citp, citp_ori, ctid, ctid_edt, citp_nt,
            id_type, org_no, linkman, linktel, linkjob, linkmail, linkphone, ceml, ctvc, crnm, crit, crit_ori, crit_nt,
            crid, crid_edt, rgdt, cls_dt, scale, country, crp_type, fud_date, reg_cptl, remark_ctvc, agency_ctnm,
            agency_citp, agency_ctid, agency_edt, remark, indu_code, stat_flag_ori, stat_flag, mer_prov, mer_city,
            mer_area,
            address, tel, mer_unit, is_line, certification, cer_num, con_acc_name, bord_flag, web_info, con_nation,
            majority_shareholder_ctnm, majority_shareholder_citp, majority_shareholder_citp_ori,
            majority_shareholder_ctid,
            majority_shareholder_edt, reg_cptl_code, bind_card, ip_code, mac_info, self_acc_no, acc_type1,
            bank_acc_name,
            reals, complex, clear, batch_pay, statement_type
        ])
        else:
            contect_data = [
            busi_reg_no, ctnm, cten, client_tp, account_tp, busi_type, smid, citp, citp_ori, ctid, ctid_edt, citp_nt,
            id_type, org_no, linkman, linktel, linkjob, linkmail, linkphone, ceml, ctvc, crnm, crit, crit_ori, crit_nt,
            crid, crid_edt, rgdt, cls_dt, scale, country, crp_type, fud_date, reg_cptl, remark_ctvc, agency_ctnm,
            agency_citp, agency_ctid, agency_edt, remark, indu_code, stat_flag_ori, stat_flag, mer_prov, mer_city,
            mer_area,
            address, tel, mer_unit, is_line, certification, cer_num, con_acc_name, bord_flag, web_info, con_nation,
            majority_shareholder_ctnm, majority_shareholder_citp, majority_shareholder_citp_ori,
            majority_shareholder_ctid,
            majority_shareholder_edt, reg_cptl_code, bind_card, ip_code, mac_info, self_acc_no, acc_type1,
            bank_acc_name,
            reals, complex, clear, batch_pay, statement_type
        ]
        #     contect_data = [
        #     busi_reg_no, ctnm, cten, client_tp, account_tp, busi_type, smid, citp, citp_ori, ctid, ctid_edt, citp_nt,
        #     id_type, org_no, linkman, linktel, linkjob, linkmail, linkphone, ceml, ctvc, crnm, crit, crit_ori, crit_nt,
        #     crid, crid_edt, rgdt, scale, country, crp_type, fud_date, reg_cptl, remark_ctvc, agency_ctnm,
        #     agency_citp, agency_ctid, agency_edt, remark, indu_code, stat_flag_ori, stat_flag, mer_prov, mer_city,
        #     mer_area,
        #     address, tel, mer_unit, is_line, certification, cer_num, con_acc_name, bord_flag, web_info, con_nation,
        #     majority_shareholder_ctnm, majority_shareholder_citp, majority_shareholder_citp_ori,
        #     majority_shareholder_ctid,
        #     majority_shareholder_edt, reg_cptl_code, bind_card, ip_code, mac_info, self_acc_no, acc_type1,
        #     bank_acc_name,
        #     reals, complex, clear, batch_pay, statement_type
        # ]

        return {
                   "busi_reg_no": busi_reg_no,
                   "ctnm": ctnm,
                   "cten": cten,
                   "client_tp": client_tp,
                   "account_tp": account_tp,
                   "busi_type": busi_type,
                   "smid": smid,
                   "citp": citp,
                   "citp_ori": citp_ori,
                   "ctid": ctid,
                   "ctid_edt": ctid_edt,
                   "citp_nt": citp_nt,
                   "id_type": id_type,
                   "org_no": org_no,
                   "linkman": linkman,
                   "linktel": linktel,
                   "linkjob": linkjob,
                   "linkmail": linkmail,
                   "linkphone": linkphone,
                   "ceml": ceml,
                   "ctvc": ctvc,
                   "crnm": crnm,
                   "crit": crit,
                   "crit_ori": crit_ori,
                   "crit_nt": crit_nt,
                   "crid": crid,
                   "crid_edt": crid_edt,
                   "rgdt": rgdt,
                   "cls_dt": cls_dt,
                   "scale": scale,
                   "country": country,
                   "crp_type": crp_type,
                   "fud_date": fud_date,
                   "reg_cptl": reg_cptl,
                   "remark_ctvc": remark_ctvc,
                   "agency_ctnm": agency_ctnm,
                   "agency_citp": agency_citp,
                   "agency_ctid": agency_ctid,
                   "agency_edt": agency_edt,
                   "remark": remark,
                   "indu_code": indu_code,
                   "stat_flag_ori": stat_flag_ori,
                   "stat_flag": stat_flag,
                   "mer_prov": mer_prov,
                   "mer_city": mer_city,
                   "mer_area": mer_area,
                   "address": address,
                   "tel": tel,
                   "mer_unit": mer_unit,
                   "is_line": is_line,
                   "certification": certification,
                   "cer_num": cer_num,
                   "con_acc_name": con_acc_name,
                   "bord_flag": bord_flag,
                   "web_info": web_info,
                   "con_nation": con_nation,
                   "majority_shareholder_ctnm": majority_shareholder_ctnm,
                   "majority_shareholder_citp": majority_shareholder_citp,
                   "majority_shareholder_citp_ori": majority_shareholder_citp_ori,
                   "majority_shareholder_ctid": majority_shareholder_ctid,
                   "majority_shareholder_edt": majority_shareholder_edt,
                   "reg_cptl_code": reg_cptl_code,
                   "bind_card": bind_card,
                   "ip_code": ip_code,
                   "mac_info": mac_info,
                   "self_acc_no": self_acc_no,
                   "acc_type1": acc_type1,
                   "bank_acc_name": bank_acc_name,
                   "reals": reals,
                   "complex": complex,
                   "clear": clear,
                   "batch_pay": batch_pay,
                   "statement_type": statement_type
               }, contect_data




if __name__ == "__main__":
    """
    4.0标准接口数据生成，同时生成9条数据
    """
    # stif_time = '20200401100000'
    # person = Person()
    # for num in range(3):
    #     info, person_contect = person.make_stan_person(num)
    #     common = CommnTable(info, person.ctif_tp, 1)
    #     t_stan_cert, cert_contect = common.make_stan_cert()
    #     t_stan_address, addr_contect = common.make_stan_address()
    #     t_stan_tel, tel_contect = common.make_stan_tel()
    #     t_stan_relation, rela_contect = common.make_stan_tel()
    #     t_stan_pact, pact_contect = common.make_stan_pact()
    #     t_stan_bact, bact_contect = common.make_stan_bact()
    #     t_stan_stif, stif_contect = common.make_stan_stif(stif_time)
    #     print(info)
    #     print(t_stan_cert)
    #     print(t_stan_address)
    #     print(t_stan_tel)
    #     print(t_stan_relation)
    #     print(t_stan_pact)
    #     print(t_stan_bact)
    #     print(t_stan_stif)

    # main7('/home/admin/make_data/data/2020-04-08_1')
    pass

