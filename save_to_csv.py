import csv
import os
import sys
from readConfig import RunConfig
config = RunConfig()

if sys.platform == 'linux':
    new_path = config.get_linux_data_path()
    if not os.path.exists(new_path):
        os.makedirs(new_path, exist_ok=False)
elif sys.platform == 'win32':
    new_path = config.get_win_data_path()
    if not os.path.exists(new_path):
        os.makedirs(new_path, exist_ok=False)
else:
    new_path = os.getcwd()


def write_to_csv(file_name, datas):

    t_stan_person_header = [u"busi_reg_no（客户号）", u"ctnm（主体名称）", u"cten拼音/英文名称", u"client_tp（客户类别(1代表客户,2代表商户)）", u"account_tp（账户分类）", u"busi_type（业务类型）", u"smid（主体特约商户编号）", u"citp证件类型", u"citp_ori(证件类型原值)", u"citp_nt证件类型说明", u"ctid证件号码", u"ctid_edt证件有效期", u"sex（性别）", u"country（国籍）", u"nation（民族）", u"birthday（出生日期）", u"education（学历）", u"ctvc（主体的行业类别）", u"picm个人年收入", u"ficm家庭年收入", u"marriage（婚姻状况）", u"ceml（电子邮件）", u"rgdt（开户日期）", u"cls_dt（销户日期）", u"remark", u"indu_code", u"stat_flag_ori客户状态原值", u"stat_flag", u"mer_prov", u"mer_city市", u"mer_area", u"address详细地址", u"tel联系电话", u"mer_unit", u"is_line", u"Certification认证渠道", u"cer_num通过身份验证渠道数量", u"con_acc_name经营名称", u"bord_flag境内外标识", u"web_info网络支付商户网址信息", u"con_nation商户所属国家或地区", u"bind_card银行绑定标识", u"ip_code注册地IP地址", u"mac_info注册设备MAC或IMEI地址", u"self_acc_no特约商户收单结算账号", u"acc_type1账户类型", u"bank_acc_name银行账户名称", u"reals客户真实有效性", u"batch_pay(批量代付标识)", u"statement_type(结算类型)"]
    t_stan_org_header = ["busi_reg_no(客户号)", "ctnm(主体名称)", "cten拼音/英文名称", "client_tp(类别(1代表客户,2代表商户)", "account_tp(账户分类)", "busi_type(业务类型)", "smid(主体特约商户编号)", "citp证件类型", "citp_ori(证件类型原值)", "ctid证件号码", "ctid_edt证件有效期", "citp_nt证件类型说明", "id_type证件类型_现场检查", "org_no组织机构代码", "linkman(联系人姓名)", "linktel(联系人手机号)", "linkjob(联系人职务)", "linkmail(联系人邮箱)", "linkphone(联系人固定电话)", "ceml(电子邮件)", "ctvc(主体的行业类别)", "crnm(主体的法定代表人姓名)", "crit(主体的法定代表人身份证件类型)", "crit_ori(主体的法定代表人身份证件类型原值)", "crit_nt(法人证件类型说明)", "crid(主体的法定代表人身份证件号码)", "crid_edt(法人证件有效期)", "rgdt(开户日期)", "cls_dt(销户日期)", "scale(企业规模)", "country(注册国家)", "crp_type机构类别", "fud_date", "reg_cptl注册资本", "remark_ctvc经营范围", "agency_ctnm代办理人姓名", "agency_citp代办理人证件类型", "agency_ctid代办理人证件号码", "agency_edt代办理人证件有效期限", "remark", "indu_code", "stat_flag_ori客户状态原值", "stat_flag", "mer_prov", "mer_city市", "mer_area", "address详细地址", "tel联系电话", "mer_unit", "is_line", "certification建立渠道", "cer_num通过身份验证渠道数量", "con_acc_name经营名称", "bord_flag境内外标识", "web_info网络支付商户网址信息", "con_nation商户所属国家或地区", "majority_shareholder_ctnm控股股东或实际控制人姓名", "majority_shareholder_citp控股股东或实际控制人证件类型", "majority_shareholder_citp_ori(控股股东或实际控制人证件类型原值)", "majority_shareholder_ctid控股股东或实际控制人证件号码", "majority_shareholder_edt控股股东或实际控制人证件有效期限", "reg_cptl_code注册资本金币种", "bind_card绑定标识", "ip_code注册地IP地址", "mac_info注册设备MAC或IMEI地址", "self_acc_no特约商户收单结算账号", "acc_type1账户类型", "bank_acc_name银行账户名称", "reals客户真实有效性", "complex非自然人结构复杂度", "clear非自然人股权可辨识度", "batch_pay(批量代付标识)", "statement_type(结算类型)"]
    t_stan_cert_header = ["ctif_id客户号", "ctif_tp客户类型1个人2机构", "citp证件类型", "citp_ori证件类型原值", "ctid_nt证件类型说明", "ctid证件号码","iss_unt证件签发机关", "address证件地址", "主体证件有效期ctid_edt", "iss_dt 证件签发日期", "iss_ctry证件签发国家", "is_rp是否主证件"]
    t_stan_address_header = ["ctif_id客户号", "ctif_tp1个人2机构", "address_tp", "address", "ctry国家代码", "prvc 省代码", "city市代码","area区县", "postcode邮编", "exp_dt失效日期", "is_rp是否主地址"]
    t_stan_tel_header = [u"ctif_id客户号", u"ctif_tp主体类型", u"tel_tp电话类型", "tel", "is_rp"]
    t_stan_relation_header = ["ctif_id客户号", "ctif_tp主体类型1个人 2机构", "rel_tp关系类型", "rel_layer关系人层级", "rel_ctif关系人客户号", "rel_cstp关系人类别 1个人2机构", "rel_name关系人姓名", "rcnt关系人国籍/国家", "citp证件类型", "citp_ori关系人证件类型原值", "ctid证件号码", "citp_nt证件类型说明", "hold_per持股比例(5)", "hold_amt持股金额", "ctid_edt证件有效期", "rel_prov关系人省", "rel_city关系人市", "rel_area关系人区县", "rear关系人详细地址", "retl关系人联系电话"]
    t_stan_pact_header = ["ctif_id", "ctif_tp主体类型1:个人2:机构", "act_tp账户类型", "act_cd账户号", "账号类别act_typ", "act_limit支付账户交易限额", "is_self_acc是否特约商户收单结算账号", "sales_name预付卡办卡人", "cst_sex预付卡办卡人性别", "nation预付卡办卡人国籍", "occupation预付卡办卡人职业", "id_type预付卡办卡人证件种类", "id_type_ori预付卡办卡人证件种类原值", "id_no预付卡办卡人证件号码", "id_deadline预付卡办卡人证件有效期截至日", "contact预付卡办卡人联系方式", "address预付卡办卡人住所地或工作单位地址", "sales_flag预付卡代直销标识", "bind_mob绑定的手机号码", "mer_unit管理机构", "cls_dt账户状态", "rgdt开户日期", "cls_stat销户日期"]
    t_stan_bact_header = ["ctif_id", "ctif_tp主体类型1:个人2:机构", "act_tp银行账号种类", "act_flag银行账号种类现场检查", "act_cd银行账户号", "cabm银行账号开户行名称", "pay_id关联支付账户", "is_self_acc是否特约商户收单结算账号", "bank_acc_name银行账户名称", "mer_unit管理机构"]
    t_stan_stif_header = ["CTIF_ID主体客户号", "CTIF_TP 1个人2 机构主体类别", "CLIENT_TP 1客户 2商户客户类别(1代表客户 2代表商户)", "SMID主体特约商户编码(30)", "CTNM", "CITP主体身份证件/证明文件类型(2)", "citp_ori主体身份证件/证明文件类型原值", "CITP_NT主体身份证件/证明文件类型说明(32)", "CTID可疑主体身份证件/证明文件号码(32)", "CBAT交易主体的银行账号种类(2)", "CBAC交易主体的银行账号(64)", "CABM交易主体银行账号的开户行名称(128)", "CTAT主体的交易账号种类（2）", "CTAC主体的交易账号（64）", "CPIN主体所在支付机构的名称（128）", "CPBA主体所在支付机构的银行账号（64）", "CPBN主体所在支付机构的银行账号的开户行名称（128）", "CTIP主体的交易IP地址", "TSTM交易时间", "CTTP货币资金转移方式（4）", "TSDR资金收付标志", "CRPP资金用途（500）", "CRTP交易币种（3）", "CRAT交易金额（接口20）", "TCIF_ID交易对手ID（100002）", "TCNM交易对手姓名/名称（128）", "TSMI交易对手特约商户编码（30）", "TCIT交易对手证件/证明文件类型（2）", "tcit_ori交易对手证件/证明文件类型原值", "TCIT_NT交易对手证件/证明文件类型说明（32）", "TCID交易对手证件/证明文件号码（32）", "TCAT交易对手的银行账号种类（）", "TCBA交易对手的银行账号（64）", "TCBN交易对手银行账号的开户行名称（128）", "TCTT交易对手的交易账号种类（2）", "TCTA交易对手的交易账号（64）", "TCPN交易对手所在支付机构的名称（128）", "TCPA交易对手所在支付机构的银行账号（64）", "TPBN交易对手所在支付机构银行账号的开户行名称（128）", "TCIP交易对手的交易IP地址（15）", "TMNM交易商品名称（64）", "BPTC银行与支付机构之间的业务交易编码（64）", "PMTC支付机构与商户之间的业务交易编码（64）", "TICD业务标识号（128）", "BUSI_TYPE", "trans_type", "pos_dev_id", "trans_stat", "bank_stat", "mer_prov", "mer_area", "pos_prov", "pos_area", "mer_unit", "extend1转换标识", "iofg 境内外标识", "trans_channel交易渠道", "ctmac交易主体发生的mac地址(32)", "balance账户余额", "acc_flag交易对方账户类型", "ctid_edt主体身份证件/证明文件有效期截止日", "tran_flag对手账号标识", "trans_order交易订单号", "trans_cst_type交易类型(客户定义) ", "crat_u交易金额折合美元", "crat_c交易金额折合人民币", "trans_way交易方式", "agency_ctnm代办人姓名", "agency_citp代办人身份证件（证明文件）类型 ", "agency_ctid代办人身份证件（证明文件）号码 ", "agency_country代办人国籍"]
    # data = "&#@".join([str(elem) for elem in datas])
    file_path = os.path.join(new_path, file_name)
    if not os.path.exists(file_path):
        head = os.path.split(file_name)[-1]
        head = head.split(".")[0]
        head = "t_stan_"+ head.split("_")[0] + "_header"
        head = "&#@".join([str(elem) for elem in eval(head)])
        # print(head)

        csvFile = open(file_path, "a", encoding="utf-8-sig", newline='')
        writer = csv.writer(csvFile)
        writer.writerows([[head], [datas]])
        csvFile.close()
    else:
        csvFile = open(file_path, "a", encoding="utf-8-sig", newline='')
        writer = csv.writer(csvFile)
        writer.writerow([datas])
        csvFile.close()


def write_to_csv_more(file_name, datas):

    t_stan_person_header = [u"busi_reg_no（客户号）", u"ctnm（主体名称）", u"cten拼音/英文名称", u"client_tp（客户类别(1代表客户,2代表商户)）", u"account_tp（账户分类）", u"busi_type（业务类型）", u"smid（主体特约商户编号）", u"citp证件类型", u"citp_ori(证件类型原值)", u"citp_nt证件类型说明", u"ctid证件号码", u"ctid_edt证件有效期", u"sex（性别）", u"country（国籍）", u"nation（民族）", u"birthday（出生日期）", u"education（学历）", u"ctvc（主体的行业类别）", u"picm个人年收入", u"ficm家庭年收入", u"marriage（婚姻状况）", u"ceml（电子邮件）", u"rgdt（开户日期）", u"cls_dt（销户日期）", u"remark", u"indu_code", u"stat_flag_ori客户状态原值", u"stat_flag", u"mer_prov", u"mer_city市", u"mer_area", u"address详细地址", u"tel联系电话", u"mer_unit", u"is_line", u"Certification认证渠道", u"cer_num通过身份验证渠道数量", u"con_acc_name经营名称", u"bord_flag境内外标识", u"web_info网络支付商户网址信息", u"con_nation商户所属国家或地区", u"bind_card银行绑定标识", u"ip_code注册地IP地址", u"mac_info注册设备MAC或IMEI地址", u"self_acc_no特约商户收单结算账号", u"acc_type1账户类型", u"bank_acc_name银行账户名称", u"reals客户真实有效性", u"batch_pay(批量代付标识)", u"statement_type(结算类型)"]
    t_stan_org_header = ["busi_reg_no(客户号)", "ctnm(主体名称)", "cten拼音/英文名称", "client_tp(类别(1代表客户,2代表商户)", "account_tp(账户分类)", "busi_type(业务类型)", "smid(主体特约商户编号)", "citp证件类型", "citp_ori(证件类型原值)", "ctid证件号码", "ctid_edt证件有效期", "citp_nt证件类型说明", "id_type证件类型_现场检查", "org_no组织机构代码", "linkman(联系人姓名)", "linktel(联系人手机号)", "linkjob(联系人职务)", "linkmail(联系人邮箱)", "linkphone(联系人固定电话)", "ceml(电子邮件)", "ctvc(主体的行业类别)", "crnm(主体的法定代表人姓名)", "crit(主体的法定代表人身份证件类型)", "crit_ori(主体的法定代表人身份证件类型原值)", "crit_nt(法人证件类型说明)", "crid(主体的法定代表人身份证件号码)", "crid_edt(法人证件有效期)", "rgdt(开户日期)", "cls_dt(销户日期)", "scale(企业规模)", "country(注册国家)", "crp_type机构类别", "fud_date", "reg_cptl注册资本", "remark_ctvc经营范围", "agency_ctnm代办理人姓名", "agency_citp代办理人证件类型", "agency_ctid代办理人证件号码", "agency_edt代办理人证件有效期限", "remark", "indu_code", "stat_flag_ori客户状态原值", "stat_flag", "mer_prov", "mer_city市", "mer_area", "address详细地址", "tel联系电话", "mer_unit", "is_line", "certification建立渠道", "cer_num通过身份验证渠道数量", "con_acc_name经营名称", "bord_flag境内外标识", "web_info网络支付商户网址信息", "con_nation商户所属国家或地区", "majority_shareholder_ctnm控股股东或实际控制人姓名", "majority_shareholder_citp控股股东或实际控制人证件类型", "majority_shareholder_citp_ori(控股股东或实际控制人证件类型原值)", "majority_shareholder_ctid控股股东或实际控制人证件号码", "majority_shareholder_edt控股股东或实际控制人证件有效期限", "reg_cptl_code注册资本金币种", "bind_card绑定标识", "ip_code注册地IP地址", "mac_info注册设备MAC或IMEI地址", "self_acc_no特约商户收单结算账号", "acc_type1账户类型", "bank_acc_name银行账户名称", "reals客户真实有效性", "complex非自然人结构复杂度", "clear非自然人股权可辨识度", "batch_pay(批量代付标识)", "statement_type(结算类型)"]
    t_stan_cert_header = ["ctif_id客户号", "ctif_tp客户类型1个人2机构", "citp证件类型", "citp_ori证件类型原值", "ctid_nt证件类型说明", "ctid证件号码","iss_unt证件签发机关", "address证件地址", "主体证件有效期ctid_edt", "iss_dt 证件签发日期", "iss_ctry证件签发国家", "is_rp是否主证件"]
    t_stan_address_header = ["ctif_id客户号", "ctif_tp1个人2机构", "address_tp", "address", "ctry国家代码", "prvc 省代码", "city市代码","area区县", "postcode邮编", "exp_dt失效日期", "is_rp是否主地址"]
    t_stan_tel_header = [u"ctif_id客户号", u"ctif_tp主体类型", u"tel_tp电话类型", "tel", "is_rp"]
    t_stan_relation_header = ["ctif_id客户号", "ctif_tp主体类型1个人 2机构", "rel_tp关系类型", "rel_layer关系人层级", "rel_ctif关系人客户号", "rel_cstp关系人类别 1个人2机构", "rel_name关系人姓名", "rcnt关系人国籍/国家", "citp证件类型", "citp_ori关系人证件类型原值", "ctid证件号码", "citp_nt证件类型说明", "hold_per持股比例(5)", "hold_amt持股金额", "ctid_edt证件有效期", "rel_prov关系人省", "rel_city关系人市", "rel_area关系人区县", "rear关系人详细地址", "retl关系人联系电话"]
    t_stan_pact_header = ["ctif_id", "ctif_tp主体类型1:个人2:机构", "act_tp账户类型", "act_cd账户号", "账号类别act_typ", "act_limit支付账户交易限额", "is_self_acc是否特约商户收单结算账号", "sales_name预付卡办卡人", "cst_sex预付卡办卡人性别", "nation预付卡办卡人国籍", "occupation预付卡办卡人职业", "id_type预付卡办卡人证件种类", "id_type_ori预付卡办卡人证件种类原值", "id_no预付卡办卡人证件号码", "id_deadline预付卡办卡人证件有效期截至日", "contact预付卡办卡人联系方式", "address预付卡办卡人住所地或工作单位地址", "sales_flag预付卡代直销标识", "bind_mob绑定的手机号码", "mer_unit管理机构", "cls_dt账户状态", "rgdt开户日期", "cls_stat销户日期"]
    t_stan_bact_header = ["ctif_id", "ctif_tp主体类型1:个人2:机构", "act_tp银行账号种类", "act_flag银行账号种类现场检查", "act_cd银行账户号", "cabm银行账号开户行名称", "pay_id关联支付账户", "is_self_acc是否特约商户收单结算账号", "bank_acc_name银行账户名称", "mer_unit管理机构"]
    t_stan_stif_header = ["CTIF_ID主体客户号", "CTIF_TP 1个人2 机构主体类别", "CLIENT_TP 1客户 2商户客户类别(1代表客户 2代表商户)", "SMID主体特约商户编码(30)", "CTNM", "CITP主体身份证件/证明文件类型(2)", "citp_ori主体身份证件/证明文件类型原值", "CITP_NT主体身份证件/证明文件类型说明(32)", "CTID可疑主体身份证件/证明文件号码(32)", "CBAT交易主体的银行账号种类(2)", "CBAC交易主体的银行账号(64)", "CABM交易主体银行账号的开户行名称(128)", "CTAT主体的交易账号种类（2）", "CTAC主体的交易账号（64）", "CPIN主体所在支付机构的名称（128）", "CPBA主体所在支付机构的银行账号（64）", "CPBN主体所在支付机构的银行账号的开户行名称（128）", "CTIP主体的交易IP地址", "TSTM交易时间", "CTTP货币资金转移方式（4）", "TSDR资金收付标志", "CRPP资金用途（500）", "CRTP交易币种（3）", "CRAT交易金额（接口20）", "TCIF_ID交易对手ID（100002）", "TCNM交易对手姓名/名称（128）", "TSMI交易对手特约商户编码（30）", "TCIT交易对手证件/证明文件类型（2）", "tcit_ori交易对手证件/证明文件类型原值", "TCIT_NT交易对手证件/证明文件类型说明（32）", "TCID交易对手证件/证明文件号码（32）", "TCAT交易对手的银行账号种类（）", "TCBA交易对手的银行账号（64）", "TCBN交易对手银行账号的开户行名称（128）", "TCTT交易对手的交易账号种类（2）", "TCTA交易对手的交易账号（64）", "TCPN交易对手所在支付机构的名称（128）", "TCPA交易对手所在支付机构的银行账号（64）", "TPBN交易对手所在支付机构银行账号的开户行名称（128）", "TCIP交易对手的交易IP地址（15）", "TMNM交易商品名称（64）", "BPTC银行与支付机构之间的业务交易编码（64）", "PMTC支付机构与商户之间的业务交易编码（64）", "TICD业务标识号（128）", "BUSI_TYPE", "trans_type", "pos_dev_id", "trans_stat", "bank_stat", "mer_prov", "mer_area", "pos_prov", "pos_area", "mer_unit", "extend1转换标识", "iofg 境内外标识", "trans_channel交易渠道", "ctmac交易主体发生的mac地址(32)", "balance账户余额", "acc_flag交易对方账户类型", "ctid_edt主体身份证件/证明文件有效期截止日", "tran_flag对手账号标识", "trans_order交易订单号", "trans_cst_type交易类型(客户定义) ", "crat_u交易金额折合美元", "crat_c交易金额折合人民币", "trans_way交易方式", "agency_ctnm代办人姓名", "agency_citp代办人身份证件（证明文件）类型 ", "agency_ctid代办人身份证件（证明文件）号码 ", "agency_country代办人国籍"]
    # data = "&#@".join([str(elem) for elem in datas])
    file_path = os.path.join(new_path, file_name)
    if not os.path.exists(file_path):
        head = os.path.split(file_name)[-1]
        head = head.split(".")[0]
        head = "t_stan_"+ head.split("_")[0] + "_header"
        head = "&#@".join([str(elem) for elem in eval(head)])
        # print(head)

        csvFile = open(file_path, "a", encoding="utf-8-sig", newline='')
        writer = csv.writer(csvFile)
        writer.writerow([head])
        writer.writerows([[data] for data in datas])
        csvFile.close()
    else:
        csvFile = open(file_path, "a", encoding="utf-8-sig", newline='')
        writer = csv.writer(csvFile)
        writer.writerows([[data] for data in datas])
        csvFile.close()


if __name__ == "__main__":
    write_to_csv("t_stan_cert.csv", "testtttttttttttttttttttttt")