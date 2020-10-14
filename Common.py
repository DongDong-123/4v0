import csv
import random
from xpinyin import Pinyin
import unicodedata
import time
import json


# from test_save import write_to_csv


def make_tel_data():
    num = 1
    while num < 100:
        ctif_id = "test_{}".format(num)
        ctif_tp = random.choice(["1", "2"])
        tel_tp = random.choice(["11", "12", "21", "22", "23"])
        tel = random.randint(13000000000, 19999999999)
        is_rp = random.choice(["1", "0"])
        tel_data = [ctif_id, ctif_tp, tel_tp, str(tel), is_rp]
        tel_data = "&#@".join(tel_data)
        num += 1
        yield tel_data


def write_to_tel():
    header = [u"ctif_id客户号", u"ctif_tp主体类型", u"tel_tp电话类型", "tel", "is_rp"]
    header = "&#@".join(header)
    tel_data = make_tel_data()
    csvFile = open("tel.csv", "w", encoding="utf-8-sig", newline='')
    writer = csv.writer(csvFile)
    writer.writerow([header])
    writer.writerows([[data] for data in tel_data])
    csvFile.close()


# write_to_tel()

def make_relation_data():
    num = 1
    while num < 100:
        ctif_id = "test_{}".format(num)
        ctif_tp = random.choice(["1", "2"])
        tel_tp = random.choice(["11", "12", "21", "22", "23"])
        tel = random.randint(13000000000, 19999999999)
        is_rp = random.choice(["1", "0"])
        tel_data = [ctif_id, ctif_tp, tel_tp, str(tel), is_rp]
        tel_data = "&#@".join(tel_data)
        num += 1
        yield tel_data


def write_to_relation():
    header = ["ctif_id客户号", "ctif_tp主体类型1个人 2机构", "rel_tp关系类型", "rel_layer关系人层级",
              "rel_ctif关系人客户号", "rel_cstp关系人类别 1个人 2机构", "rel_name关系人姓名", "rcnt关系人国籍/国家",
              "citp证件类型", "citp_ori关系人证件类型原值", "ctid证件号码", "citp_nt证件类型说明", "hold_per持股比例(5)",
              "hold_amt持股金额", "ctid_edt证件有效期", "rel_prov关系人省", "rel_city关系人市", "rel_area关系人区县",
              "rear关系人详细地址", "retl关系人联系电话"]
    header = "&#@".join(header)
    tel_data = make_tel_data()
    csvFile = open("relation.csv", "w", encoding="utf-8-sig", newline='')
    writer = csv.writer(csvFile)
    writer.writerow([header])
    writer.writerows([[data] for data in tel_data])
    csvFile.close()


# 拼接数据
def make_connect_data(one_data):
    """
    接收数据，使用特定的字符拼接，返回拼接好的数据
    :param one_data:
    :return:
    """
    datas = ['' if elem == None else elem for elem in one_data ]
    data = "&#@".join([str(elem) for elem in datas])
    # print(data)
    return data


code_list_temp = []


# 行业代码数据
def make_indu_code_data():
    """
    行业代码数据
    :return:
    """
    global code_list_temp
    if not code_list_temp:
        with open("incustry_code.txt", "r", encoding="utf-8") as f:
            codes = f.read()
            code_list_temp = codes.split(",")

    return random.choice(code_list_temp)


province_code_list_temp = ""


# write_to_relation()

# -------------------------分界线----------------------------

# def read_province_data():
#     """
#     读取、处理源数据文件，生成省市区代码，保存到新文件，运行一次即可注释掉，使用新文件获取数据
#     :return:
#     """
#     with open("province_code.txt", 'r', encoding="utf-8") as f:
#         res = f.read()
#
#     # print(len(res), res, type(res))
#     res_list = res.split(",")
#
#     # 处理数据
#     extent_data = []
#     province = []
#     city = []
#     print(len(res_list))
#     for elem in res_list:
#         if elem[2:] == "0000" and elem not in ["710000", "810000", "820000"]:
#             res_list.remove(elem)
#             province.append(elem)
#         elif elem[-2:] == "00" and elem[:2] not in ["11", "12", "31", "50", "71", "81", "82"]:
#             res_list.remove(elem)
#             city.append(elem)
#
#     # print(len(res_list))
#
#     for x in res_list:
#         with open("province_code2.txt", 'w', encoding="utf-8") as f:
#             f.write(x + ',')
#
#     # print(len(res_list))
#     return random.choice(res_list)

# 生成省市区代码
def make_province_code_data():
    """
    读取处理好的数据文件,只包含县级数据
    :return:区域代码
    """
    global province_code_list_temp
    if not province_code_list_temp:
        with open("province_code2.txt", 'r', encoding="utf-8") as f:
            res = f.read()
            province_code_list_temp = res.split(",")
    data = random.choice(province_code_list_temp)
    return data
    # return res_list


# print(make_province_code_data())

# def compare_area_data():
#     with open("province_code1.txt", 'r', encoding="utf-8") as f:
#         res = f.read()
#     res_list = res.split(",")
#     return res_list


# 生成省市区地址
# def read_excel():
#     """
#     从源文件中读取省市代码表，提取省市县二级地址，写入新文件，并且根据传入字符串参数code，返回对应的地址，
#     第一次运行，后续可从新文件中读取
#     :return:
#     """
#     booksheet = xlrd.open_workbook(r'省市号码表.xlsx')
#     ret1 = booksheet.sheets()[0]
#     # ret2 = booksheet.sheet_by_index(0)  # 按索引找sheet
#     row_num = ret1.nrows
#     # print(row_num)
#     data_dict = {}
#     # 指定分词缓存路径及文件名
#     path = os.getcwd()
#     jieba.dt.tmp_dir = path
#     jieba.dt.cache_file = "jieba.cache"
#     jieba.load_userdict("userdict.txt")
#     jieba.suggest_freq(('上海市', '浦东新区'), True)
#     for num in range(1, row_num):
#         row_data = ret1.row_values(num)
#         # print(row_data[0], type(row_data[0]),row_data[1], type(row_data[1]))
#         elem = str(int(row_data[0]))
#         # print(elem,type(elem))
#         if elem[2:] == "0000" and elem not in ["710000", "810000", "820000"]:
#             # print(elem)
#             continue
#         elif elem[-2:] == "00" and elem[:2] not in ["11", "12", "31", "50", "71", "81", "82"]:
#             # print(elem)
#             continue
#         else:
#             data_list = [data for data in jieba.cut(row_data[1], cut_all=False)]
#             data_dict[elem] = data_list
#             print(data_list)
#     # print(len(data_dict), data_dict)
#     with open("data2.txt", "w", encoding="utf-8") as f:
#         f.write(json.dumps(data_dict, ensure_ascii=False))
#
#     # return data_dict.get(code, "空")

# 提取省码表，写入文件
# def write_province_data():
#     """
#     提取省码表，写入文件,一次性函数
#     :return:
#     """
#     booksheet = xlrd.open_workbook(r'省市号码表.xlsx')
#     ret1 = booksheet.sheets()[0]
#     # ret2 = booksheet.sheet_by_index(0)  # 按索引找sheet
#     row_num = ret1.nrows
#     # print(row_num)
#     data_dict = {}
#
#     for num in range(1, row_num):
#         row_data = ret1.row_values(num)
#         # print(row_data[0], type(row_data[0]),row_data[1], type(row_data[1]))
#         elem = str(int(row_data[0]))
#         # print(elem,type(elem))
#         if elem[2:] == "0000":
#             print(elem)
#             data_dict[elem] = row_data[1]
#
#         # elif elem[-2:] == "00" and elem[:2] not in ["11", "12", "31", "50", "71", "81", "82"]:
#         #     # print(elem)
#         #     continue
#         else:
#             continue
#     # print(len(data_dict), data_dict)
#     with open("province_data.txt", "w", encoding="utf-8") as f:
#         f.write(json.dumps(data_dict, ensure_ascii=False))
#
#     # return data_dict.get(code, "空")


# 临时变量
province_data_list_temp = ''


# 返回省名数据
def get_province_data(code):
    """
    根据省市代码的前2位，后4位默认全为0，获取省级名称
    :param code: 2位字符串（数字)
    :return: 省名
    """
    global province_data_list_temp
    if not province_data_list_temp:
        with open("province_data.txt", "r", encoding="utf-8") as f:
            province_data_temp = f.read()
            province_data_list_temp = json.loads(province_data_temp)

    data = province_data_list_temp.get(code[:2] + "0000")
    if not data:
        data = ''
    return data


# 返回省代码
def get_province_code(code):
    """

    :param code: 县代码
    :return: 省代码
    """
    code = code[:2] + "0000"
    return code


# 省代码临时存放
province_code_temp = ''


# 省代码
def get_province_code_data(code):
    """

    :param code:
    :return:
    """
    global province_code_temp
    if not province_code_temp:
        with open("province_data.txt", "r", encoding="utf-8") as f:
            province_code_temp = f.read()
            province_data = json.loads(province_code_temp)

    else:
        province_data = json.loads(province_code_temp)

    if code == "999999" or "990000":
        province_code = "999999"
    elif province_data.get(code[:2] + "0000"):
        province_code = code[:2] + "0000"
    else:
        # print(code)
        raise ValueError("code error,code={}".format(code))
    return province_code


province_city_data_temp = ""


# 市县地址数据
def make_province_city_data(code):
    """
    获取省市县地址
    :param code: 省市县代码，字符串格式
    :return: 返回对应的市、县拆分的中文列表
    """
    global province_city_data_temp
    if not province_city_data_temp:
        with open("data2.txt", 'r', encoding="utf-8") as f:
            file = f.read()
            province_city_data_temp = json.loads(file)
    data = province_city_data_temp.get(code)

    return data


province_city_code_temp = ""


# 市代码
def make_province_city_code_data(code):
    """
    code的前4位加后两位00，组成市代码，验证存在后返回，不存在，直接返回空
    :param code:
    :return:
    """
    global province_city_code_temp
    code2 = code[:4] + "00"
    if not province_city_code_temp:
        with open("city_code.txt", 'r', encoding="utf-8") as f:
            res = f.read()
            province_city_code_temp = res.split(",")
    if code2 in province_city_code_temp:
        return code2
    else:
        return ""


# # 县代码
# def make_province_xian_code_data(code):
#     with open("province_code2.txt", 'r', encoding="utf-8") as f:
#         res = f.read()
#     res_list = res.split(",")
#     if code in res_list:
#         return res_list


# 市县数据
def make_province_city_process_data(code):
    """
    中间处理函数，插入省，拼接省市县,
    :param code:
    :return:返回省市县
    """
    # print(code)
    province = get_province_data(code[:2])
    # print(province)
    data = make_province_city_data(code)
    # print("2", data)
    # if not data:
    #     data = ["{}无市数据".format(code), "{}无县数据".format(code)]
    if not province:
        province = ""
    if province == data[0]:
        data = "".join(data)
    else:
        data.insert(0, province)
        data = "".join(data)
    return data


# print(make_province_city_process_data('110000'))


# # 返回县数据
# def make_province_data(data):
#     """
#     根据传入县市名，使用分词工具分词，提取最后一级
#     :param data:
#     :return: 县级市或区
#     """
#     cut_data = jieba.cut(data, cut_all=False)
#     data_list = [word for word in cut_data]
#     # print(data_list)
#     return data_list[-1]


# 生成证件类型数据
def make_cert_type_data():
    return random.choice([
        "11",  # 居民身份证或临时身份证
        "12",  # 军人或武警身份证件
        "13",  # 港澳台通行证
        "14",  # 外国公民护照
        "19",  # 其他个人有效证件(需进一步说明)
        "21",  # 组织机构代码
        "29"  # 其他机构代码(需进一步说明)
    ])


# 生成姓名拼音数据
def word_to_pinyin(word):
    """
    中文姓名转换为拼音
    :param word: 中文姓名
    :return: 中文全拼
    """
    if word:
        word_pinyin = Pinyin()
        result = word_pinyin.get_pinyin(u"{}".format(word), tone_marks='marks')
        # 去除声调
        res = unicodedata.normalize('NFKD', result).encode('ascii', 'ignore')
        res = res.decode()
        res = res.replace("-", "")  # 去除连接符
        return res
    else:
        raise ValueError("姓名为空，无法转换成拼音！")


# 身份证号数据
def make_ctid_data():
    """
    # 生成身份证号，年龄范围，1940年至2003年出生的人，
    """
    area_extent = make_province_code_data()  # 地区号段
    age_extent = str(random.randint(1940, 2003))  # 年区间，
    month_extent = str(random.randint(1, 12))  # 月区间
    if eval(month_extent) < 10:
        month_extent = "0" + month_extent
    # 日区间，2月特殊处理，计28天，其他自然月按30天
    if month_extent == "2":
        day_extent = str(random.randint(1, 28))
    else:
        day_extent = str(random.randint(1, 30))
    if eval(day_extent) < 10:
        day_extent = "0" + day_extent
    # 4位尾号
    tail_extent = str(random.randint(1, 9999))
    if len(tail_extent) < 4:
        tail_extent = (4 - len(tail_extent)) * "0" + tail_extent

    return "".join([area_extent, age_extent, month_extent, day_extent, tail_extent])


# 生成随机数
def make_random_num(num):
    """ 接收int类型参数num，根据参数随机生成数字,返回字符串"""
    res_list = []
    while len(res_list) < num:
        elem = random.randint(0, 9)
        if res_list or elem:
            res_list.append(str(elem))

    return "".join(res_list)


# 身份证有效期数据
def make_Card_valid_date(IDCard):
    """生成身份证有效期数据，1965年以后出生的身份证有效期,在今年的基础上随机增加/减少年数，80年之前的，有效期长期"""
    if int(IDCard[6:10]) > 1965:
        date = random.choice(["-2", "-1", "0", "1", "3", "5", "10", "15", "20"])
        year = time.strftime("%Y", time.localtime())
        born_month_day = IDCard[10:14]  # 拼接月、日
        born_year = str(int(year) + int(date))
        return born_year + born_month_day
    else:
        return "99991231"


# 性别数据
def make_sex(IDCard):
    """根据身份证号区分性别,性别号码为0时，设置性别未知、保密"""
    code = int(IDCard[17:18])
    if code == 0:
        return "0"  # 未知/保密
    elif code % 2 == 0:
        return "1"  # 男
    else:
        return "2"  # 女


# 随机生成生日
def make_birthday_data():
    age_extent = str(random.randint(1940, 2003))  # 年区间，
    month_extent = str(random.randint(1, 12))  # 月区间
    if eval(month_extent) < 10:
        month_extent = "0" + month_extent
    # 日区间，2月特殊处理，计28天，其他自然月按30天
    if month_extent == "2":
        day_extent = str(random.randint(1, 28))
    else:
        day_extent = str(random.randint(1, 30))
    if eval(day_extent) < 10:
        day_extent = "0" + day_extent

    return '-'.join([age_extent, month_extent, day_extent])


# 国家数据
def choice_contry():
    """
    国家，暂时默认为中国
    :return:
    """
    country = random.choice([
        "CHN", "ALB", "DZA", "AFG", "ARG", "ARE", "ABW", "OMN", "AZE", "EGY", "ETH", "IRL", "EST", "AND", "AGO", "AIA",
        "ATG", "AUT", "ALA", "AUS", "MAC", "BRB", "PNG", "BHS", "PAK", "PRY", "PSE", "BHR", "PAN", "BRA", "BLR", "BMU",
        "BGR", "MNP", "BEN", "BEL", "ISL", "PRI", "BIH", "POL", "BOL", "BLZ", "BWA", "BTN", "BFA", "BDI", "BVT", "PRK",
        "GNQ", "DNK", "DEU", "TLS", "TGO", "DOM", "DMA", "RUS", "ECU", "ERI", "FRA", "FRO", "PYF", "GUF", "ATF", "MAF",
        "VAT", "PHL", "FJI", "FIN", "CPV", "GMB", "COG", "COD", "COL", "CRI", "GRD", "GRL", "GEO", "GGY", "CUB", "GLP",
        "GUM", "GUY", "KAZ", "HTI", "KOR", "NLD", "BES", "SXM", "HMD", "MNE", "HND", "KIR", "DJI", "KGZ", "GIN", "GNB",
        "CAN", "GHA", "GAB", "KHM", "CZE", "ZWE", "CMR", "QAT", "CYM", "CCK", "COM", "CIV", "KWT", "HRV", "KEN", "COK",
        "CUW", "LVA", "LSO", "LAO", "LBN", "LTU", "LBR", "LBY", "LIE", "REU", "LUX", "RWA", "ROU", "MDG", "IMN", "MDV",
        "FLK", "MLT", "MWI", "MYS", "MLI", "MKD", "MHL", "MTQ", "MYT", "MUS", "MRT", "USA", "UMI", "ASM", "VIR", "MNG",
        "MSR", "BGD", "PER", "FSM", "MMR", "MDA", "MAR", "MCO", "MOZ", "MEX", "NKR", "NAM", "ZAF", "ATA", "SGS", "SSD",
        "NRU", "NPL", "NIC", "NER", "NGA", "NIU", "NOR", "NFK", "PLW", "PCN", "PRT", "JPN", "SWE", "CHE", "SLV", "WSM",
        "SRB", "SLE", "SEN", "CYP", "SYC", "SAU", "BLM", "CXR", "STP", "SHN", "KNA", "LCA", "SMR", "SPM", "VCT", "LKA",
        "SVK", "SVN", "SJM", "SWZ", "SDN", "SUR", "SLB", "SOM", "TJK", "THA", "TZA", "TON", "TCA", "TTO", "TUN", "TUV",
        "TUR", "TKM", "TKL", "WLF", "VUT", "GTM", "VEN", "BRN", "UGA", "UKR", "URY", "UZB", "ESP", "ESH", "GRC", "HKG",
        "SGP", "NCL", "NZL", "HUN", "SYR", "JAM", "ARM", "YEM", "IRQ", "IRN", "ISR", "ITA", "IND", "IDN", "GBR", "VGB",
        "IOT", "JOR", "VNM", "ZMB", "JEY", "TCD", "GIB", "CHL", "CAF", "TWN"
        ])
    # country = "CHN"
    return country


# 客户状态数据
def make_stat_flag_data(cls_dt):
    """
    如果销户日期为真，设置客户状态为关闭，
    n-正常， c-关闭
    :param cls_dt: 销户日期
    :return:客户状态标识
    """

    if cls_dt:
        stat_flag = "c"
    else:
        stat_flag = "n"
    return stat_flag

    # return 'n'  # 默认正常


# 开户日期数据
def make_register_date():
    """
    生成开户日期,日期范围，本年本月本日起至前推20年之间，
    :return:
    """
    year_now = eval(time.strftime("%Y", time.localtime()))
    num = random.randint(0, 20)
    year = year_now - num
    month_now = int(time.strftime("%m", time.localtime()))
    if year == year_now:
        month = random.randint(1, month_now)
    else:
        month = random.randint(1, 12)
    if month < 10:
        month = "0" + str(month)
    else:
        month = str(month)
    day_now = int(time.strftime("%d", time.localtime()))
    if month == "02":
        if year == year_now and int(month) == month_now:
            day = str(random.randint(1, day_now))
        else:
            day = str(random.randint(1, 28))
    else:
        if year == year_now and int(month) == month_now:
            day = str(random.randint(1, day_now))
        else:
            day = str(random.randint(1, 30))
    if eval(day) < 10:
        day = "0" + day

    return str(year) + month + day


# 有效期数据
def make_enable_date():
    """
    生成开户日期,日期范围，本年本月本日起至前推20年之间，
    :return:
    """
    year_now = eval(time.strftime("%Y", time.localtime()))
    num = random.randint(-2, 20)
    year = year_now + num
    month_now = int(time.strftime("%m", time.localtime()))
    if year == year_now:
        month = random.randint(1, month_now)
    else:
        month = random.randint(1, 12)
    if month < 10:
        month = "0" + str(month)
    else:
        month = str(month)
    day_now = int(time.strftime("%d", time.localtime()))
    if month == "02":
        if year == year_now and int(month) == month_now:
            day = str(random.randint(1, day_now))
        else:
            day = str(random.randint(1, 28))
    else:
        if year == year_now and int(month) == month_now:
            day = str(random.randint(1, day_now))
        else:
            day = str(random.randint(1, 30))
    if eval(day) < 10:
        day = "0" + day

    return str(year) + month + day


# 地址数据
def make_address(code):
    """

    :param code: 省市区代码
    :return: 详细地址
    """
    three_level_addr = make_province_city_process_data(code)
    street_address = random.choice([
        "解放路", "千佛山", "趵突泉", "泉城路", "大明湖", "东关", "文东", "建新", "甸柳", "燕山", "姚家", "龙洞", "智远", "舜华路", "大观园", "杆石桥", "四里村",
        "魏家庄", "二七", "七里山", "六里山", "舜玉路", "泺源", "王官庄", "舜耕", "白马山", "七贤", "十六里河", "兴隆", "党家", "陡沟", "振兴街", "中大槐树",
        "道德街", "西市场", "五里沟", "营市街", "青年公园", "南辛庄", "段店北路", "张庄路", "匡山", "美里湖", "吴家堡", "腊山", "兴福", "玉清湖", "无影山", "天桥东街",
        "北村", "南村", "堤口路", "北坦", "制锦市", "宝华", "官扎营", "纬北路", "药山", "北园", "泺口", "桑梓店", "大桥", "山大路", "洪家楼", "东风", "全福",
        "孙村", "巨野河", "华山", "荷花路", "王舍人", "鲍山", "郭店", "唐冶", "港沟", "遥墙", "临港", "仲宫", "柳埠", "董家", "彩石", "文昌", "崮云湖", "平安",
        "五峰山", "归德", "万德", "张夏", "明水", "双山", "圣井", "埠村", "枣园", "龙山", "普集", "官庄", "相公庄", "绣惠", "文祖", "曹范", "白云湖", "高官寨",
        "宁家埠", "济阳", "济北", "回河", "孙耿", "崔寨", "太平", "榆山", "锦水"
    ])
    areas_name = random.choice([
        "万豪国际公寓", "晓月苑", "永定路商住中心", "橙色年代", "嘉慧苑", "致雅居", "彩虹城", "松园小区", "燕归园", "北京青年城", "金宝纯别墅", "翌景嘉园", "涧桥·泊屋馆",
        "京东丽景", "旭风苑公寓", "朝阳无限", "庄胜二期", "潇雅居", "GOGO新世代", "飞腾家园", "英嘉公寓", "高第", "金榜园", "迎曦园", "风格与林", "太阳国际公馆(瑞景嘉园)",
        "永合馨苑", "澳洲新星", "丰润世家", "洋桥花园", "长安新城", "金隅丽港城", "兴涛社区", "糖人街", "时代芳群", "运河园", "浉城百郦", "测试项目", "新洲商务大厦", "加来小镇",
        "新新公寓", "颍泽洲", "城市印象", "上河美墅", "同泰苑", "和枫雅居", "建兴家园", "昊腾花园", "高苑·花样年华", "金码大厦", "天辉公寓", "NOLITA那里", "政馨家园",
        "文林商苑", "蝶翠华庭", "晋元庄小区", "幸福源", "当代城市家园", "非常生活", "祥瑞苑", "雪梨澳乡", "清欣园", "晟丰阁", "倚林佳园", "华龙小区", "秀安园", "新华联锦园",
        "乐澜宝邸", "棉花城", "CLASS", "金宸公寓", "燕景佳园", "珠江帝景", "龙山新新小镇", "万景公寓", "飘HOME", "蓝堡", "新纪元公寓", "中信红树湾", "海德堡花园",
        "天缘公寓", "长城盛世mm", "鲁艺上河村", "瑞馨公寓", "鼎诚国际MM", "德胜世嘉", "榆园新居", "远洋天地", "星河城", "黎明新座", "世纪城", "大观园中华商住区", "中国第一商城",
        "后现代城", "中海凯旋", "新都丽苑", "陶然北岸", "观河锦苑", "星光公寓", "观筑", "绿城星洲花园", "御鹿家园", "都市心海岸", "山水汇豪", "漪内轩", "颐园(碧水云天)",
        "新荣家园", "双桥温泉北里住宅", "恬心家园", "正邦嘉园", "依翠园", "万科西山庭院", "新御景", "天行建商务大厦", "浉城百丽", "华腾园", "同仁园", "格林小镇",
        "东华经典(东华金座)", "俊景苑", "朗琴园", "快乐洋城", "新中环公寓", "非常宿舍", "清城名苑", "兴都苑(水榭楼台)", "雍景台", "风林绿洲(奕翠庭)", "团结公寓"
    ])
    building_name = str(random.randint(1, 50))
    unit_num = str(random.randint(1, 9))
    floor_num = str(random.randint(1, 30))
    room_num = str(random.randint(1, 4))
    return three_level_addr + street_address + "街道" + areas_name + building_name + "楼" + unit_num + "单元" + floor_num + "层" + room_num + "号"


# 电话号码数据
def make_tel_num(tp=11):
    """
    随机生成手机号码
    11:家庭电话
    12:工作电话
    21:固定电话
    22:移动电话
    23:传真
    :return: 返回字符串类型
    """
    if tp == '21' or tp == '23':
        phone = make_random_num(9)
    else:
        one_two = random.choice(["13", "14", "15", "16", "17", "18", "19"])
        three_nine = []
        for num in range(9):
            elem = random.randint(0, 9)
            three_nine.append(str(elem))
        phone = one_two + "".join(three_nine)
    return phone


def make_random_str(num):
    str_col = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    res = ""
    for elem in range(num):
        res += random.choice(str_col)
    return res


# 邮箱数据
def make_email_data():
    extend = random.choice(["163.com", "126.com", "gmail.com", "qq.com", "sina.com", "outlook.com"])
    name_1 = make_random_str(6)
    name_2 = make_random_num(6)
    return name_1 + "_" + name_2 + "@" + extend


# ip地址数据
def make_ip_data(busi_type):
    """
    生成ip数据，当支付类型为网络支付时，生成ip并返回，否则，返回空
    :param busi_type:
    :return:
    """
    if busi_type == "01":
        first = random.randint(1, 254)
        if first in [10, 127, 192, 172]:
            first += 1
        second = random.randint(0, 255)
        third = random.randint(0, 255)
        fouth = random.randint(1, 255)
        ip = ".".join([str(first), str(second), str(third), str(fouth)])
    else:
        ip = ''
    return ip


# 生成姓名数据
def make_name_data(longth=None):
    first_name = random.choice([
        "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈", "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许", "何", "吕",
        "施", "张", "孔", "曹", "严", "华", "金", "魏", "陶", "姜", "戚", "谢", "邹", "喻", "柏", "水", "窦", "章", "云", "苏", "潘", "葛",
        "奚", "范", "彭", "郎", "鲁", "韦", "昌", "马", "苗", "凤", "花", "方", "俞", "任", "袁", "柳", "酆", "鲍", "史", "唐", "费", "廉",
        "岑", "薛", "雷", "贺", "倪", "汤", "滕", "殷", "罗", "毕", "郝", "邬", "安", "常", "乐", "于", "时", "傅", "皮", "卞", "齐", "康",
        "伍", "余", "元", "卜", "顾", "孟", "平", "黄", "和", "穆", "萧", "尹", "姚", "邵", "湛", "汪", "祁", "毛", "禹", "狄", "米", "贝",
        "明", "臧", "计", "伏", "成", "戴", "谈", "宋", "茅", "庞", "熊", "纪", "舒", "屈", "项", "祝", "董", "粱", "杜", "阮", "蓝", "闵",
        "席", "季", "麻", "强", "贾", "路", "娄", "危", "江", "童", "颜", "郭", "梅", "盛", "林", "刁", "钟", "徐", "邱", "骆", "高", "夏",
        "蔡", "田", "樊", "胡", "凌", "霍", "虞", "万", "支", "柯", "昝", "管", "卢", "莫", "经", "房", "裘", "缪", "干", "解", "应", "宗",
        "丁", "宣", "贲", "邓", "郁", "单", "杭", "洪", "包", "诸", "左", "石", "崔", "吉", "钮", "龚", "程", "嵇", "邢", "滑", "裴", "陆",
        "荣", "翁", "荀", "羊", "於", "惠", "甄", "麴", "家", "封", "芮", "羿", "储", "靳", "汲", "邴", "糜", "松", "井", "段", "富", "巫",
        "乌", "焦", "巴", "弓", "牧", "隗", "山", "谷", "车", "侯", "宓", "蓬", "全", "郗", "班", "仰", "秋", "仲", "伊", "宫", "宁", "仇",
        "栾", "暴", "甘", "钭", "厉", "戎", "祖", "武", "符", "刘", "景", "詹", "束", "龙", "叶", "幸", "司", "韶", "郜", "黎", "蓟", "薄",
        "印", "宿", "白", "怀", "蒲", "邰", "从", "鄂", "索", "咸", "籍", "赖", "卓", "蔺", "屠", "蒙", "池", "乔", "阴", "欎", "胥", "能",
        "苍", "双", "闻", "莘", "党", "翟", "谭", "贡", "劳", "逄", "姬", "申", "扶", "堵", "冉", "宰", "郦", "雍", "舄", "璩", "桑", "桂",
        "濮", "牛", "寿", "通", "边", "扈", "燕", "冀", "郏", "浦", "尚", "农", "温", "别", "庄", "晏", "柴", "瞿", "阎", "充", "慕", "连",
        "茹", "习", "宦", "艾", "鱼", "容", "向", "古", "易", "慎", "戈", "廖", "庾", "终", "暨", "居", "衡", "步", "都", "耿", "满", "弘",
        "匡", "国", "文", "寇", "广", "禄", "阙", "东", "殴", "殳", "沃", "利", "蔚", "越", "夔", "隆", "师", "巩", "厍", "聂", "晁", "勾",
        "敖", "融", "冷", "訾", "辛", "阚", "那", "简", "饶", "空", "曾", "毋", "沙", "乜", "养", "鞠", "须", "丰", "巢", "关", "蒯", "相",
        "查", "後", "荆", "红", "游", "竺", "权", "逯", "盖", "益", "桓", "公", "万俟", "司马", "上官", "欧阳", "夏侯", "诸葛", "闻人", "东方",
        "赫连", "皇甫", "尉迟", "公羊", "澹台", "公冶", "宗政", "濮阳", "淳于", "单于", "太叔", "申屠", "公孙", "仲孙", "轩辕", "令狐", "钟离", "宇文",
        "长孙", "慕容", "鲜于", "闾丘", "司徒", "司空", "亓官", "司寇", "仉", "督", "子车", "颛孙", "端木", "巫马", "公西", "漆雕", "乐正", "壤驷", "公良",
        "拓跋", "夹谷", "宰父", "谷梁", "晋", "楚", "闫", "法", "汝", "鄢", "涂", "钦", "段干", "百里", "东郭", "南门", "呼延", "归", "海", "羊舌",
        "微生", "岳", "帅", "缑", "亢", "况", "后", "有", "琴", "梁丘", "左丘", "东门", "西门", "商", "牟", "佘", "佴", "伯", "赏", "南宫", "墨",
        "哈", "谯", "笪", "年", "爱", "阳", "佟", "第五", "言", "福", "卓", "蔺", "屠", "蒙", "池", "乔", "阳", "郁", "胥", "能", "苍", "双",
        "闻", "莘", "党", "翟", "谭", "贡", "劳", "逄", "姬", "申", "扶", "堵", "冉", "宰", "郦", "雍", "却", "璩", "桑", "桂", "濮", "牛",
        "寿", "通", "边", "扈", "燕", "冀", "僪", "浦", "尚", "农", "温", "别", "庄", "晏", "柴", "瞿", "阎", "充", "慕", "连", "茹", "习",
        "宦", "艾", "鱼", "容", "向", "古", "易", "慎", "戈", "庾", "终", "暨", "居", "衡", "步都", "耿", "满", "弘", "匡", "国", "文", "寇",
        "广", "禄", "阙", "东欧", "殳", "沃", "利", "蔚", "越", "夔", "隆", "师", "巩", "厍", "聂晁", "勾", "敖", "融", "冷", "訾", "辛", "阚",
        "那", "简", "饶", "空曾", "毋", "沙", "乜", "养", "鞠", "须", "丰", "巢", "关", "蒯", "相查", "后", "荆", "红", "游", "竺", "权", "逮",
        "盍", "益", "桓", "公", "唱"])
    second_name = random.choice([
        "一", "是", "我", "不", "在", "人", "们", "有", "来", "他", "这", "上", "着", "个", "地", "到",
        "大", "里", "说", "去", "子", "得", "也", "和", "那", "要", "下", "看", "天", "时", "过", "出",
        "小", "么", "起", "你", "都", "把", "好", "还", "多", "没", "为", "又", "可", "家", "学", "只",
        "以", "主", "会", "样", "年", "想", "能", "生", "同", "老", "中", "从", "自", "面", "前", "头",
        "到", "它", "后", "然", "走", "很", "像", "见", "两", "用", "国", "动", "进", "成", "回", "什",
        "边", "作", "对", "开", "而", "已", "些", "现", "山", "民", "候", "经", "发", "工", "向", "事",
        "命", "给", "长", "水", "义", "三", "声", "于", "高", "正", "手", "知", "理", "眼", "志", "点",
        "心", "战", "二", "问", "但", "身", "方", "实", "做", "叫", "当", "住", "听", "革", "打", "呢",
        "真", "党", "全", "才", "四", "已", "所", "敌", "之", "最", "光", "产", "情", "路", "分", "总",
        "条", "白", "话", "东", "席", "次", "亲", "如", "被", "花", "口", "放", "儿", "常", "西", "气",
        "五", "第", "使", "写", "军", "吧", "文", "运", "在", "果", "怎", "定", "许", "快", "明", "行",
        "因", "别", "飞", "外", "树", "物", "活", "部", "门", "无", "往", "船", "望", "新", "带", "队",
        "先", "力", "完", "间", "却", "站", "代", "员", "机", "更", "九", "每", "风", "级", "跟", "笑",
        "啊", "孩", "万", "少", "直", "意", "夜", "比", "阶", "连", "车", "重", "便", "斗", "马", "哪",
        "化", "太", "指", "变", "社", "似", "士", "者", "干", "石", "满", "决", "百", "原", "群",
        "究", "各", "六", "本", "思", "解", "立", "河", "爸", "村", "八", "难", "早", "论", "根",
        "共", "让", "相", "研", "今", "其", "书", "接", "应", "关", "信", "觉", "步", "反", "处",
        "记", "将", "千", "找", "争", "领", "或", "师", "结", "块", "跑", "谁", "草", "越", "字", "加",
        "紧", "爱", "等", "习", "阵", "月", "青", "半", "火", "法", "题", "建", "赶", "位",
        "唱", "海", "七", "任", "件", "感", "准", "张", "团", "屋", "离", "片", "科", "倒", "睛", "利",
        "世", "刚", "且", "由", "送", "切", "星", "晚", "表", "够", "整", "认", "响", "雪", "流", "未",
        "场", "该", "并", "底", "深", "刻", "平", "伟", "忙", "提", "确", "近", "亮", "轻", "讲", "农",
        "古", "黑", "告", "界", "拉", "名", "呀", "土", "清", "阳", "照", "办", "史", "改", "历", "转",
        "画", "造", "嘴", "此", "治", "北", "必", "服", "雨", "穿", "内", "识", "验", "传", "业", "菜", "兴"])
    last_name = random.choice([
        "命", "给", "长", "水", "义", "三", "声", "于", "高", "正", "手", "知", "理", "眼", "志", "点",
        "心", "战", "二", "问", "但", "身", "方", "实", "做", "叫", "当", "住", "听", "革", "打", "呢",
        "真", "党", "全", "才", "四", "已", "所", "敌", "之", "最", "光", "产", "情", "路", "分", "总",
        "条", "白", "话", "东", "席", "次", "亲", "如", "被", "花", "口", "放", "儿", "常", "西", "气",
        "五", "第", "使", "写", "军", "吧", "文", "运", "在", "果", "怎", "定", "许", "快", "明", "行",
        "因", "别", "飞", "外", "树", "物", "活", "部", "门", "无", "往", "船", "望", "新", "带", "队",
        "先", "力", "完", "间", "却", "站", "代", "员", "机", "更", "九", "每", "风", "级", "跟", "笑",
        "啊", "孩", "万", "少", "直", "意", "夜", "比", "阶", "连", "车", "重", "便", "斗", "马", "哪",
        "化", "太", "指", "变", "社", "似", "士", "者", "干", "石", "满", "决", "百", "原", "群",
        "究", "各", "六", "本", "思", "解", "立", "河", "爸", "村", "八", "难", "早", "论", "根",
        "共", "让", "相", "研", "今", "其", "书", "接", "应", "关", "信", "觉", "步", "反", "处",
        "记", "将", "千", "找", "争", "领", "或", "师", "结", "块", "跑", "谁", "草", "越", "字"])
    fouth_name = random.choice([
        "命", "给", "长", "水", "义", "三", "声", "于", "高", "正", "手", "知", "理", "眼", "志", "点",
        "心", "战", "二", "问", "但", "身", "方", "实", "做", "叫", "当", "住", "听", "革", "打", "呢",
        "真", "党", "全", "才", "四", "已", "所", "敌", "之", "最", "光", "产", "情", "路", "分", "总",
        "条", "白", "话", "东", "席", "次", "亲", "如", "被", "花", "口", "放", "儿", "常", "西", "气",
        "五", "第", "使", "写", "军", "吧", "文", "运", "在", "果", "怎", "定", "许", "快", "明", "行",
        "因", "别", "飞", "外", "树", "物", "活", "部", "门", "无", "往", "船", "望", "新", "带", "队",
        "先", "力", "完", "间", "却", "站", "代", "员", "机", "更", "九", "每", "风", "级", "跟", "笑",
        "啊", "孩", "万", "少", "直", "意", "夜", "比", "阶", "连", "车", "重", "便", "斗", "马", "哪",
        "五", "第", "使", "写", "军", "吧", "文", "运", "在", "果", "怎", "定", "许", "快", "明", "行",
        "因", "别", "飞", "外", "树", "物", "活", "部", "门", "无", "往", "船", "望", "新", "带", "队",
        "先", "力", "完", "间", "却", "站", "代", "员", "机", "更", "九", "每", "风", "级", "跟", "笑",
        "啊", "孩", "万", "少", "直", "意", "夜", "比", "阶", "连", "车", "重", "便", "斗", "马", "哪",
        "化", "太", "指", "变", "社", "似", "士", "者", "干", "石", "满", "决", "百", "原", "群",
        "究", "各", "六", "本", "思", "解", "立", "河", "爸", "村", "八", "难", "早", "论", "根",
        "共", "让", "相", "研", "今", "其", "书", "接", "应", "关", "信", "觉", "步", "反", "处",
    ])
    if longth == 4:
        return first_name + second_name + last_name + fouth_name
    else:
        return first_name + second_name + last_name

from faker import Faker
fake = Faker(locale='zh_CN')
def org_name_data():
    """ 机构名称"""
    word = ["命", "给", "长", "水", "义", "三", "声", "于", "高", "正", "手", "知", "理", "眼", "志", "点",
        "心", "战", "二", "问", "但", "身", "方", "实", "做", "叫", "当", "住", "听", "革", "打", "呢",
        "真", "党", "全", "才", "四", "已", "所", "敌", "之", "最", "光", "产", "情", "路", "分", "总",
        "条", "白", "话", "东", "席", "次", "亲", "如", "被", "花", "口", "放", "儿", "常", "西", "气",
        "五", "第", "使", "写", "军", "吧", "文", "运", "在", "果", "怎", "定", "许", "快", "明", "行",
        "因", "别", "飞", "外", "树", "物", "活", "部", "门", "无", "往", "船", "望", "新", "带", "队",
        "先", "力", "完", "间", "却", "站", "代", "员", "机", "更", "九", "每", "风", "级", "跟", "笑",
        "啊", "孩", "万", "少", "直", "意", "夜", "比", "阶", "连", "车", "重", "便", "斗", "马", "哪",
        "化", "太", "指", "变", "社", "似", "士", "者", "干", "石", "满", "决", "百", "原", "群",
        "究", "各", "六", "本", "思", "解", "立", "河", "爸", "村", "八", "难", "早", "论", "根",
        "共", "让", "相", "研", "今", "其", "书", "接", "应", "关", "信", "觉", "步", "反", "处",
        "记", "将", "千", "找", "争", "领", "或", "师", "结", "块", "跑", "谁", "草", "越", "字"]
    end_name = fake.company()
    word_list = []
    for i in range(4):
        word_list.append(random.choice(word))
    return "".join(word_list) + end_name

# 支付账号数据
def make_act_cd_data(act_tp):
    # 互联网支付账号，虚拟账户号
    """
    生成支付账号
    :param act_tp: 账号类型
    :return: 随机账号
    """
    if act_tp == "11":
        act_cd = make_random_num(13)
    elif act_tp == "211":
        act_cd = make_random_num(19)
    elif act_tp == "212":
        act_cd = make_random_num(15)
    else:
        raise TypeError("act_tp类型错误！")
    return act_cd


# 账号等级数据
def make_act_type_data(act_tp):
    """
    随机生成账号等级，提高高级别账号权重
    :param act_tp: 账号类型
    :return: 随机账号类别标识码，高级别账号权重较大
    """
    if act_tp == "11":
        act_typ = random.choice(["3", "3", "3", "2", "2", "1"])
    elif act_tp == "211":
        act_typ = random.choice(["1", "1", "1", "2", "2", "3"])
    elif act_tp == "212":
        act_typ = "1"
    else:
        raise TypeError("act_tp类型错误！")
    return act_typ


# 限额数据
def make_act_limit_data(act_tp, act_typ):
    """
    限额
    :param act_tp:
    :param act_typ:
    :return:
    """
    if act_tp == "11":
        if act_typ == "3":
            act_limit = float(100000)
        elif act_typ == "2":
            act_limit = float(10000)
        elif act_typ == "1":
            act_limit = float(1000)
        else:
            raise TypeError("act_tye类型错误！")
    elif act_tp == "211":
        if act_typ == "1":
            act_limit = float(100000)
        elif act_typ == "2":
            act_limit = float(10000)
        elif act_typ == "3":
            act_limit = float(1000)
        else:
            raise TypeError("act_tye类型错误！")
    elif act_tp == "212":
        act_limit = float(1000)
    else:
        raise TypeError("act_tp类型错误！")

    return act_limit


# 预付卡数据
def make_prepaid_card_data(infos):
    """
    如果业务类型为预付卡，生成预付卡数据，否则，置空
    :param infos: person/org表信息
    :return:预付卡数据
    """
    if infos.get("busi_type") == "03":
        sales_name = infos.get("ctnm")
        cst_sex = infos.get("sex", "0")
        nation = infos.get("country")
        occupation = "职业待定"  # 待定，是否有职业表
        id_type = infos.get("citp")
        id_type_ori = infos.get("citp_ori")
        id_no = infos.get("ctid")
        id_deadline = infos.get("ctid_edt")
        contact = infos.get("tel")
        address = infos.get("address")
        sales_flag = random.choice(["11", "12"])
    else:
        sales_name = ""
        cst_sex = ""
        nation = ""
        occupation = ""
        id_type = ""
        id_type_ori = ""
        id_no = ""
        id_deadline = ""
        contact = ""
        address = ""
        sales_flag = ""

    return sales_name, cst_sex, nation, occupation, id_type, id_type_ori, id_no, id_deadline, contact, address, sales_flag


# 互联网账号数据
def make_bind_mob_data(infos):
    """
    如果是互联网账号，返回手机号，否则，值为空
    :param infos:
    :return: bind_mob值
    """
    if infos.get("busi_type") == "01":
        bind_mob = infos.get("tel")
    else:
        bind_mob = ""
    return bind_mob


# 银行账号种类数据
def make_bank_act_tp_data(ctif_tp):
    """
    生成银行账号种类
    :param ctif_tp: 主体类型
    :return: 账号类型
    """
    if ctif_tp == "1":
        act_tp = random.choice(["02", "03"])
    elif ctif_tp == "2":
        act_tp = random.choice(["01", "03"])
    else:
        raise TypeError("ctif_tp={}类型错误！".format(ctif_tp))

    return act_tp


# 开户行数据
def make_cabm_data(code):
    """
    根据身份证所属省市，随机生成开户行名称
    :param code: 身份证号前6位字符串
    :return: 开户行名称
    """
    province = make_province_city_process_data(code)
    street_address = random.choice([
        "解放路", "千佛山", "趵突泉", "泉城路", "大明湖", "东关", "文东", "建新", "甸柳", "燕山", "姚家", "龙洞", "智远", "舜华路", "大观园", "杆石桥", "四里村",
        "魏家庄", "二七", "七里山", "六里山", "舜玉路", "泺源", "王官庄", "舜耕", "白马山", "七贤", "十六里河", "兴隆", "党家", "陡沟", "振兴街", "中大槐树",
        "道德街", "西市场", "五里沟", "营市街", "青年公园", "南辛庄", "段店北路", "张庄路", "匡山", "美里湖", "吴家堡", "腊山", "兴福", "玉清湖", "无影山", "天桥东街",
        "北村", "南村", "堤口路", "北坦", "制锦市", "宝华", "官扎营", "纬北路", "药山", "北园", "泺口", "桑梓店", "大桥", "山大路", "洪家楼", "东风", "全福",
        "孙村", "巨野河", "华山", "荷花路", "王舍人", "鲍山", "郭店", "唐冶", "港沟", "遥墙", "临港", "仲宫", "柳埠", "董家", "彩石", "文昌", "崮云湖", "平安",
        "五峰山", "归德", "万德", "张夏", "明水", "双山", "圣井", "埠村", "枣园", "龙山", "普集", "官庄", "相公庄", "绣惠", "文祖", "曹范", "白云湖", "高官寨",
        "宁家埠", "济阳", "济北", "回河", "孙耿", "崔寨", "太平", "榆山", "锦水"
    ])
    bank_name = random.choice(["工商银行", "建设银行", "邮政储蓄银行", "招商银行", "光大银行", "农业银行", "中国银行", "浦发银行", "浙商银行"])
    return province + bank_name + street_address + "支行"


# 交易时间数据
def make_trade_time_data():
    # 全局变量，交易日期与文件名日期一致
    global file_date_time
    # trade_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
    trade_year = time.strftime("%Y", time.localtime())
    trade_month = time.strftime("%m", time.localtime())
    trade_date = time.strftime("%d", time.localtime())
    # trade_year = str(eval(trade_year))
    # trade_month = str(eval(trade_month))
    trade_date = "17"  # 暂时默认为14号
    # trade_date = str(eval(trade_date)-11)
    file_date_time = "-".join([trade_year, trade_month, trade_date])
    trade_time = trade_year + trade_month + trade_date + "090000"
    # trade_time = trade_time.replace("25", "14")
    return trade_time


# 货币资金转移方式数据
def make_cttp_data():
    cttp = random.choice([
        "0000",  # 即向支付账户充值——通过银行账户
        "0001",  # 即向支付账户充值——通过仅限线上实名支付账户充值的预付卡
        "0100",  # 即互联网支付——付款方银行账户向收款方银行账户转出
        "0101",  # 即互联网支付——付款方银行账户向收款方支付账户转出
        "0102",  # 即互联网支付——付款方支付账户向收款方支付账户转出
        "0103",  # 即互联网支付——付款方支付账户向收款方银行账户转出
        "0200",  # 即移动电话支付、固定电话支付、数字电视支付——付款方银行账户向收款方银行账户转出
        "0201",  # 即移动电话支付、固定电话支付、数字电视支付——付款方银行账户向收款方支付账户转出
        "0202",  # 即移动电话支付、固定电话支付、数字电视支付——付款方支付账户向收款方支付账户转出
        "0203",  # 即移动电话支付、固定电话支付、数字电视支付——付款方支付账户向收款方银行账户转出
        "0300",  # 即从支付账户结算提款
        "0400",  # 即预付卡发行业务——现金购买
        "0401",  # 即预付卡发行业务——银行转账方式购买
        "0500",  # 即预付卡充值业务——通过现金充值
        "0501",  # 即预付卡充值业务——通过银行账户充值
        "0600",  # 即预付卡受理业务
        "0700",  # 即预付卡赎回业务——现金赎回
        "0701",  # 即预付卡赎回业务——银行转账方式赎回
        "0800"  # 即银行卡收单业务
    ])
    return cttp


# 交易金额数据
def make_crat_data():
    """
    随机生成交易金额，保留2位小数
    :return:
    """
    amount_1 = random.uniform(10000, 10000000)
    return round(amount_1, 2)


# 行业类别数据(机构专用）
def make_org_ctvc_data():
    ctvc = random.choice([
        "2A",  # 农、林、牧、渔业
        "2B",  # 采矿业
        "2C",  # 制造业
        "2D",  # 电力、燃气及水的生产和供应业
        "2E",  # 建筑业
        "2F",  # 交通运输、仓储和邮政业
        "2G",  # 信息传输、计算机服务和软件业
        "2H",  # 批发和零售业
        "2I",  # 住宿和餐饮业
        "2J",  # 金融业
        "2K",  # 房地产业
        "2L",  # 租赁和商务服务业
        "2M",  # 科学研究、技术服务和地质勘查业
        "2N",  # 水利、环境和公共设施管理业
        "2O",  # 居民服务和其他服务业
        "2P",  # 教育
        "2Q",  # 卫生、社会保障和社会福利业
        "2R",  # 文化、体育和娱乐业
        "2S",  # 公共管理和社会组织
        "2T"  # 国际组织
    ])
    return ctvc


# 业务识别号数据
def make_ticd_data():
    """
    交易流水的唯一识别码，时间戳加地区代码加5位随机数字
    :return:
    """
    timestmp = time.time()
    area_code = make_province_code_data()
    ticd = str(timestmp).replace(".", "") + area_code + make_random_num(5)
    return ticd


# mac设备数据
def make_mac_info_data(busi_type):
    """银行卡收单不需要"""
    if busi_type == "02":
        mac_info = ""
    else:
        mac_info = '-'.join(
            [make_random_str(2), make_random_num(2), make_random_str(2), make_random_num(2), make_random_str(2),
             make_random_num(2)])
    return mac_info


# 业务类型数据
def make_busi_type_data():
    busi_type = random.choice([
        "01",  # 互联网支付
        "02",  # 银行卡收单
        "03"  # 预付卡发行与受理
        # "04",  # 移动电话支付
        # "05",  # 固定电话支付
        # "06",  # 数字电视支付
        # "07"  # 货币汇兑
    ])
    return busi_type


# 交易类型数据
def make_trans_type_data(busi_type):
    """
    根据业务类型生成交易类型数据
    :param busi_type:
    :return:
    """
    if busi_type == "01":
        trans_type = random.choice([
            "A10",  # 消费收单
            "A11",  # 退货退款
            "A12",  # 提现
            "A13",  # 转账
            "A14",  # 充值
            "A15",  # 消费
            "A16",  # 其他
            "A17",  # 代收
            "A18",  # 代付
            "A19",  # 批量代付
            "A20",  # 条码消费
            "A21",  # 条码转账
            "A80",  # D0、T0消费收单
            "A90"  # D0、T0条码消费收单
        ])
    elif busi_type == "03":
        trans_type = random.choice([
            "B11",  # 购买
            "B12",  # 充值
            "B13",  # 赎回
            "B14"  # 消费
        ])
    elif busi_type == "02":
        trans_type = random.choice([
            "C11",  # 取现
            "C12",  # 预授权完成（请求）
            "C13",  # MOTO预授权完成（请求）
            "C14",  # 预授权完成（通知）
            "C15",  # MOTO预授权完成（通知）
            "C16",  # 手工预授权完成
            "C17",  # 手工MOTO预授权完成
            "C18",  # 消费
            "C19",  # 消费（分期付款）
            "C20",  # 消费（积分）
            "C21",  # MOTO消费
            "C22",  # MOTO退货
            "C23",  # 退货（联机）
            "C24",  # 存款（或存款确认）
            "C25",  # 余额查询
            "C26",  # 汇款（仅指汇款交易本身而不包括汇款验证交易）
            "C27",  # 手工汇款
            "C28",  # 磁条卡现金充值（充值确认）
            "C29",  # 脱机消费（仅受理方包含）
            "C30",  # 代收
            "C31",  # 代付（代付确认）
            "C32",  # 撤销
            "C33",  # 冲正
            "C90"  # D0、T0消费
        ])
    else:
        trans_type = random.choice([
            "D11",  # 委托付款
            "D12",  # 受益收款
            "D13",  # 委托转让
            "D14",  # 委托受让
            "D15",  # 委托赎回
            "D16",  # 运用拨款
            "D17",  # 运用收款
            "D18",  # 运用转让
            "D19"  # 提前还款
        ])

    return trans_type


# IMEI设备标识数据
def make_pos_dev_id_data(busi_type):
    """
    网络支付业务必须填，其他业务，应填
    :param busi_type:交易类型
    :return: IMEI数据
    """
    if busi_type == "01":
        pos_dev_id = make_random_num(10)
    else:
        pos_dev_id = random.choice([make_random_num(10), ""])

    return pos_dev_id


# 交易渠道数据
def make_trans_channel_data():
    """
    随机生成交易渠道
    :return:
    """
    trans_channel = random.choice([
        "01",  # 支付账户
        "02",  # 银行账户
        "03",  # 网关支付
        "04",  # 快捷支付
        "05",  # 银联通道
        "06"  # 网联渠道
    ])
    return trans_channel


# 账户类型数据
def make_acc_flag_data(busi_type):
    if busi_type == "01":  # 网络支付
        acc_flag = random.choice([
            "A11",  # 银行账户
            "A12",  # 支付账户
            "A13"  # 预付卡账户
        ])
    elif busi_type == "03":  # 预付卡支付
        acc_flag = random.choice([
            "B11",  # 银行账户
            "B12",  # 支付账户
            "B13",  # 预付卡账户
        ])
    elif busi_type == "02":  # 银行收单
        acc_flag = random.choice([
            "C11",  # 借记卡
            "C12"  # 贷记卡
        ])
    else:
        acc_flag = ""
    return acc_flag


# 交易类型代码数据
def make_trans_cst_type_data():
    """根据客户自己的交易类型，建立字典"""
    trans_cst_type = random.choice([
        "000000",  # 人民币-现钞-银行汇票
        "000001",  # 人民币-现钞-银行承兑汇票
        "000002",  # 人民币-现钞-商业承兑汇票
        "000003",  # 人民币-现钞-本票
        "000004",  # 人民币-现钞-支票
        "000010",  # 人民币-现钞-银行卡
        "000011",  # 人民币-现钞-境外银行卡（非柜台方式交易）
        "000012",  # 人民币-现钞-银行卡（通过POS）
        "000013",  # 人民币-现钞-银行卡（通过ATM 且跨行交易）
        "000014",  # 人民币-现钞-银行卡（通过ATM 且非跨行交易）
        "000015",  # 人民币-现钞-境外银行卡（非柜台方式交易—通过POS）
        "000016",  # 人民币-现钞-境外银行卡（非柜台方式交易—通过ATM）
        "000017",  # 人民币-现钞-境外银行卡（柜台方式交易）
        "000020",  # 人民币-现钞-网上支付
        "000021",  # 人民币-现钞-移动支付
        "000030",  # 人民币-现钞-现金支付
        "000031",  # 人民币-现钞-汇兑
        "000032",  # 人民币-现钞-托收承付
        "000033",  # 人民币-现钞-委托收款
        "000034",  # 人民币-现钞-定期借记
        "000035",  # 人民币-现钞-定期贷记
        "000036",  # 人民币-现钞-国内信用证
        "000051",  # 人民币-现钞-其他
        "000100",  # 人民币-非现钞
        "000101",  # 人民币-非现钞-银行承兑汇票
        "000102",  # 人民币-非现钞-商业承兑汇票
        "000103",  # 人民币-非现钞-本票
        "000104",  # 人民币-非现钞-支票
        "000110",  # 人民币-非现钞-银行卡
        "000111",  # 人民币-非现钞-境外银行卡（非柜台方式交易）
        "000112",  # 人民币-非现钞-银行卡（通过POS）
        "000113",  # 人民币-非现钞-银行卡（通过ATM 且跨行交易）
        "000114",  # 人民币-非现钞-银行卡（通过ATM 且非跨行交易）
        "000115",  # 人民币-非现钞-境外银行卡（非柜台方式交易—通过POS）
        "000116",  # 人民币-非现钞-境外银行卡（非柜台方式交易—通过ATM）
        "000117",  # 人民币-非现钞-境外银行卡（柜台方式交易）
        "000120",  # 人民币-非现钞-网上支付
        "000121",  # 人民币-非现钞-移动支付
        "000130",  # 人民币-非现钞-现金支付
        "000131",  # 人民币-非现钞-汇兑
        "000132",  # 人民币-非现钞-托收承付
        "000133",  # 人民币-非现钞-委托收款
        "000134",  # 人民币-非现钞-定期借记
        "000135",  # 人民币-非现钞-定期贷记
        "000136",  # 人民币-非现钞-国内信用证
        "000151",  # 人民币-非现钞-其他
        "010001",  # 外币-现金
        "010002",  # 外币-现金结售汇
        "010003",  # 外币-现钞兑换
        "010004",  # 外币-其他现金
        "010005",  # 外币-境外银行卡（非柜台方式现金交易）
        "010101",  # 外币-信用证
        "010102",  # 外币-托收
        "010103",  # 外币-保函
        "010104",  # 外币-电汇
        "010105",  # 外币-票汇
        "010106",  # 外币-信汇
        "010107",  # 外币-其他非现金支付
        "010108",  # 外币-境外银行卡（非柜台方式非现金交易）
        "010006",  # 外币-境外银行卡（非柜台方式现金交易—通过 ATM）
        "010007",  # 外币-境外银行卡（柜台方式现金交易）
        "010109",  # 外币-境外银行卡（非柜台方式非现金交易—通过 ATM）
        "010110",  # 外币-境外银行卡（非柜台方式非现金交易—通过 POS）
        "010111",  # 外币-旅行支票
        "020001",  # 代理西联国际汇款（现金业务）
        "020002",  # 代理速汇金国际汇款（现金业务）
        "020003",  # 代理银星速汇国际汇款（现金业务）
        "020004",  # 代理侨汇通国际汇款（现金业务）
        "020005",  # 代理 Paystone国际汇款（现金业务）
        "020099",  # 代理其他公司国际汇款（现金业务）
        "020101",  # 代理西联国际汇款（非现金业务）
        "020102",  # 代理速汇金国际汇款（非现金业务）
        "020103",  # 代理银星速汇国际汇款（非现金业务）
        "020104",  # 代理侨汇通国际汇款（非现金业务）
        "020105",  # 代理 Paystone国际汇款（非现金业务）
        "020199",  # 代理其他公司国际汇款（非现金业务）
        "030100",  # 通过人民币账户购汇并转账
        "030110",  # 通过外币账户结汇并转账
        "030120"  # 本外币间兑换并转账
    ])
    return trans_cst_type


# 代办人国籍数据
def make_country_data():
    agency_country = random.choice([
        "CHN",  # 中国
        "ALB",  # 阿尔巴尼亚
        "DZA",  # 阿尔及利亚
        "AFG",  # 阿富汗
        "ARG",  # 阿根廷
        "ARE",  # 阿联酋
        "ABW",  # 阿鲁巴
        "OMN",  # 阿曼
        "AZE",  # 阿塞拜疆
        "EGY",  # 埃及
        "ETH",  # 埃塞俄比亚
        "IRL",  # 爱尔兰
        "EST",  # 爱沙尼亚
        "AND",  # 安道尔
        "AGO",  # 安哥拉
        "AIA",  # 安圭拉
        "ATG",  # 安提瓜和巴布达
        "AUT",  # 奥地利
        "ALA",  # 奥兰群岛
        "AUS",  # 澳大利亚
        "MAC",  # 澳门
        "BRB",  # 巴巴多斯
        "PNG",  # 巴布亚新几内亚
        "BHS",  # 巴哈马
        "PAK",  # 巴基斯坦
        "PRY",  # 巴拉圭
        "PSE",  # 巴勒斯坦
        "BHR",  # 巴林
        "PAN",  # 巴拿马
        "BRA",  # 巴西
        "BLR",  # 白俄罗斯
        "BMU",  # 百慕大
        "BGR",  # 保加利亚
        "MNP",  # 北马里亚纳群岛
        "BEN",  # 贝宁
        "BEL",  # 比利时
        "ISL",  # 冰岛
        "PRI",  # 波多黎各
        "BIH",  # 波黑
        "POL",  # 波兰
        "BOL",  # 玻利维亚
        "BLZ",  # 伯利兹
        "BWA",  # 博茨瓦纳
        "BTN",  # 不丹
        "BFA",  # 布基纳法索
        "BDI",  # 布隆迪
        "BVT",  # 布韦岛
        "PRK",  # 朝鲜
        "GNQ",  # 赤道几内亚
        "DNK",  # 丹麦
        "DEU",  # 德国
        "TLS",  # 东帝汶
        "TGO",  # 多哥
        "DOM",  # 多米尼加
        "DMA",  # 多米尼克
        "RUS",  # 俄罗斯
        "ECU",  # 厄瓜多尔
        "ERI",  # 厄立特里亚
        "FRA",  # 法国
        "FRO",  # 法罗群岛
        "PYF",  # 法属波利尼西亚
        "GUF",  # 法属圭亚那
        "ATF",  # 法属南部领地
        "MAF",  # 法属圣马丁
        "VAT",  # 梵蒂冈
        "PHL",  # 菲律宾
        "FJI",  # 斐济群岛
        "FIN",  # 芬兰
        "CPV",  # 佛得角
        "GMB",  # 冈比亚
        "COG",  # 刚果（布）
        "COD",  # 刚果（金）
        "COL",  # 哥伦比亚
        "CRI",  # 哥斯达黎加
        "GRD",  # 格林纳达
        "GRL",  # 格陵兰
        "GEO",  # 格鲁吉亚
        "GGY",  # 根西岛
        "CUB",  # 古巴
        "GLP",  # 瓜德罗普
        "GUM",  # 关岛
        "GUY",  # 圭亚那
        "KAZ",  # 哈萨克斯坦
        "HTI",  # 海地
        "KOR",  # 韩国
        "NLD",  # 荷兰
        "BES",  # 荷兰加勒比区
        "SXM",  # 荷属圣马丁
        "HMD",  # 赫德岛和麦克唐纳群岛
        "MNE",  # 黑山
        "HND",  # 洪都拉斯
        "KIR",  # 基里巴斯
        "DJI",  # 吉布提
        "KGZ",  # 吉尔吉斯斯坦
        "GIN",  # 几内亚
        "GNB",  # 几内亚比绍
        "CAN",  # 加拿大
        "GHA",  # 加纳
        "GAB",  # 加蓬
        "KHM",  # 柬埔寨
        "CZE",  # 捷克
        "ZWE",  # 津巴布韦
        "CMR",  # 喀麦隆
        "QAT",  # 卡塔尔
        "CYM",  # 开曼群岛
        "CCK",  # 科科斯群岛
        "COM",  # 科摩罗
        "CIV",  # 科特迪瓦
        "KWT",  # 科威特
        "HRV",  # 克罗地亚
        "KEN",  # 肯尼亚
        "COK",  # 库克群岛
        "CUW",  # 库拉索
        "LVA",  # 拉脱维亚
        "LSO",  # 莱索托
        "LAO",  # 老挝
        "LBN",  # 黎巴嫩
        "LTU",  # 立陶宛
        "LBR",  # 利比里亚
        "LBY",  # 利比亚
        "LIE",  # 列支敦士登
        "REU",  # 留尼汪
        "LUX",  # 卢森堡
        "RWA",  # 卢旺达
        "ROU",  # 罗马尼亚
        "MDG",  # 马达加斯加
        "IMN",  # 马恩岛
        "MDV",  # 马尔代夫
        "FLK",  # 马尔维纳斯群岛（福克兰）
        "MLT",  # 马耳他
        "MWI",  # 马拉维
        "MYS",  # 马来西亚
        "MLI",  # 马里
        "MKD",  # 马其顿
        "MHL",  # 马绍尔群岛
        "MTQ",  # 马提尼克
        "MYT",  # 马约特
        "MUS",  # 毛里求斯
        "MRT",  # 毛里塔尼亚
        "USA",  # 美国
        "UMI",  # 美国本土外小岛屿
        "ASM",  # 美属萨摩亚
        "VIR",  # 美属维尔京群岛
        "MNG",  # 蒙古国；蒙古
        "MSR",  # 蒙塞拉特岛
        "BGD",  # 孟加拉国
        "PER",  # 秘鲁
        "FSM",  # 密克罗尼西亚联邦
        "MMR",  # 缅甸
        "MDA",  # 摩尔多瓦
        "MAR",  # 摩洛哥
        "MCO",  # 摩纳哥
        "MOZ",  # 莫桑比克
        "MEX",  # 墨西哥
        "NKR",  # 纳戈尔诺-卡拉巴赫
        "NAM",  # 纳米比亚
        "ZAF",  # 南非
        "ATA",  # 南极洲
        "SGS",  # 南乔治亚岛和南桑威奇群岛
        "SSD",  # 南苏丹
        "NRU",  # 瑙鲁
        "NPL",  # 尼泊尔
        "NIC",  # 尼加拉瓜
        "NER",  # 尼日尔
        "NGA",  # 尼日利亚
        "NIU",  # 纽埃
        "NOR",  # 挪威
        "NFK",  # 诺福克岛
        "PLW",  # 帕劳
        "PCN",  # 皮特凯恩群岛
        "PRT",  # 葡萄牙
        "JPN",  # 日本
        "SWE",  # 瑞典
        "CHE",  # 瑞士
        "SLV",  # 萨尔瓦多
        "WSM",  # 萨摩亚
        "SRB",  # 塞尔维亚
        "SLE",  # 塞拉利昂
        "SEN",  # 塞内加尔
        "CYP",  # 塞浦路斯
        "SYC",  # 塞舌尔
        "SAU",  # 沙特阿拉伯
        "BLM",  # 圣巴泰勒米岛
        "CXR",  # 圣诞岛
        "STP",  # 圣多美和普林西比
        "SHN",  # 圣赫勒拿
        "KNA",  # 圣基茨和尼维斯
        "LCA",  # 圣卢西亚
        "SMR",  # 圣马力诺
        "SPM",  # 圣皮埃尔和密克隆
        "VCT",  # 圣文森特和格林纳丁斯
        "LKA",  # 斯里兰卡
        "SVK",  # 斯洛伐克
        "SVN",  # 斯洛文尼亚
        "SJM",  # 斯瓦尔巴群岛和扬马延岛
        "SWZ",  # 斯威士兰
        "SDN",  # 苏丹
        "SUR",  # 苏里南
        "SLB",  # 所罗门群岛
        "SOM",  # 索马里
        "TJK",  # 塔吉克斯坦
        "THA",  # 泰国
        "TZA",  # 坦桑尼亚
        "TON",  # 汤加
        "TCA",  # 特克斯和凯科斯群岛
        "TTO",  # 特立尼达和多巴哥
        "TUN",  # 突尼斯
        "TUV",  # 图瓦卢
        "TUR",  # 土耳其
        "TKM",  # 土库曼斯坦
        "TKL",  # 托克劳
        "WLF",  # 瓦利斯和富图纳
        "VUT",  # 瓦努阿图
        "GTM",  # 危地马拉
        "VEN",  # 委内瑞拉
        "BRN",  # 文莱
        "UGA",  # 乌干达
        "UKR",  # 乌克兰
        "URY",  # 乌拉圭
        "UZB",  # 乌兹别克斯坦
        "ESP",  # 西班牙
        "ESH",  # 西撒哈拉
        "GRC",  # 希腊
        "HKG",  # 香港
        "SGP",  # 新加坡
        "NCL",  # 新喀里多尼亚
        "NZL",  # 新西兰
        "HUN",  # 匈牙利
        "SYR",  # 叙利亚
        "JAM",  # 牙买加
        "ARM",  # 亚美尼亚
        "YEM",  # 也门
        "IRQ",  # 伊拉克
        "IRN",  # 伊朗
        "ISR",  # 以色列
        "ITA",  # 意大利
        "IND",  # 印度
        "IDN",  # 印尼
        "GBR",  # 英国
        "VGB",  # 英属维尔京群岛
        "IOT",  # 英属印度洋领地
        "JOR",  # 约旦
        "VNM",  # 越南
        "ZMB",  # 赞比亚
        "JEY",  # 泽西岛
        "TCD",  # 乍得
        "GIB",  # 直布罗陀
        "CHL",  # 智利
        "CAF",  # 中非
        "TWN"  # 中国台湾
    ])
    return agency_country


# 身份证件类型
def make_citp_data():
    citp = random.choice([
        "11",  # 居民身份证或临时身份证
        "12",  # 军人或武警身份证件
        "13",  # 港澳台通行证
        "14",  # 外国公民护照
        "19"  # 其他个人有效证件(需进一步说明) 中国公民护照填写'19'
    ])
    return citp


# 企业规模数据
def make_scale_data():
    scale = random.choice([
        "1",  # 500人以上
        "2",  # 200-500人
        "3",  # 10-200人
        "4"  # 10人及以下
    ])
    return scale


# 组织机构类别数据
def make_crp_type_data():
    crp_type = random.choice([
        "1",  # 企业
        "2",  # 机关
        "3",  # 事业单位
        "4",  # 社会团体
        "5"  # 其他组织机构
    ])
    return crp_type





# 交易对手id数据
def make_tcif_id_data(busi_type):
    """
    交易对手id，不适用于收单，当交易类型为银行收单时返回空
    :param busi_type:
    :return:
    """
    if busi_type == "02":
        tcif_id = ''
    else:
        tcif_id = make_random_num(22)
    return tcif_id


# 特约商户收单结算账号数据
def make_self_acc_no_data(client_tp):
    """
    非商户不填（网络支付、预付卡、银行卡收单均需填写）
    :param client_tp:
    :return:
    """
    if client_tp == "02":
        self_acc_no = make_random_num(20)
    else:
        self_acc_no = ""
    return self_acc_no


# 账户类型数据
def make_acc_type1_data(client_tp):
    """
    非商户不填（网络支付、预付卡、银行卡收单均需填写）
    :param client_tp:
    :return:
    """
    if client_tp == "02":
        acc_type1 = random.choice(["11", "12"])
    else:
        acc_type1 = ""
    return acc_type1


# 银行账户名称数据
def make_bank_acc_name_data(acc_type1):
    """
    当acc_type1=12时填写，银行账号对应账户名称（网络支付、预付卡、银行卡收单均需填写）
    :param acc_type1:
    :return:
    """
    if acc_type1 == "12":
        bank_acc_name = "XXX信息科技有限公司"
    else:
        bank_acc_name = ""
    return bank_acc_name


# 对手账号标识数据
def make_tran_flag_data(busi_type):
    """
    对手账号标识数据，仅需网络支付填写，其他不填
    :param busi_type:
    :return:
    """
    if busi_type == "01":
        tran_flag = random.choice(["11", "12"])
    else:
        tran_flag = ''
    return tran_flag


# 交易订单号数据
def make_trans_order_data(busi_type):
    """
    交易订单号，仅需网络支付填写
    :param busi_type:
    :return:
    """
    if busi_type == "01":
        trans_order = make_random_num(20)
    else:
        trans_order = ""

    return trans_order




# 批量代付标识数据
def make_batch_pay_data(busi_type, client_tp):
    """
    批量代付标识,预付卡业务不填
    :param busi_type:
    :return:
    """
    if busi_type == "03":
        batch_pay = ""
    elif client_tp == "1":
        batch_pay = "2"
    else:
        batch_pay = random.choice(["1", "2"])
    return batch_pay


# 网络支付商户网址信息数据
def make_web_info_data(busi_type):
    """
    网络支付商户网址信息、与商户签约时登记注册网址，非网络支付业务，以及无网址的商户可不填,
    默认网络用户都填了
    :param busi_type:
    :return:
    """
    if busi_type == "01":
        web_info = "http://www.baidu.com"
    else:
        web_info = ""
    return web_info


# 地址类型数据
def make_address_tp_data():
    address_tp = random.choice([
        "11",  # 家庭地址
        "12",  # 工作地址
        "13",  # 证件地址
        "21",  # 注册地址
        "22",  # 经营地址
        "99"  # 其它地址
    ])
    return address_tp


# 支付账户表账户状态数据
def make_pact_cls_dt_data(name):
    """
    非正常数据各造一条，其他为正常数据
    "C",  # 注销
    "N",  # 正常
    "I",  # 待激活
    "U",  # 闲置
    "B"  # 冻结
    :param name:
    :return:
    """
    num = name.split("_")
    if num[-1] == "1":
        cls_dt = "C"
    elif num[-1] == "2":
        cls_dt = "I"
    elif num[-1] == "3":
        cls_dt = "U"
    elif num[-1] == "4":
        cls_dt = "B"
    else:
        cls_dt = "N"
    return cls_dt


# 个人/机构表销户日期数据
def make_cls_dt_data(name):
    """
    个人表销户日期数据,使用随机生成注册日期函数生成日期,默认当客户号尾号数字为1时，账户是销户
    :param name:
    :return:
    """
    # num = name.split("_")
    # if num[-1] in [str(num) for num in range(1, 1000000, 100)]:
    #     cls_dt = make_register_date()
    # else:
    #     cls_dt = ""
    cls_dt = ""
    return cls_dt


# 账户分类数据
def make_account_tp_data(busi_type):
    if busi_type in ["02", "03"]:
        account_tp = ""
    else:
        account_tp = random.choice(["1", "2", "3"])
    return account_tp


# 管理机构数据
def make_mer_unit_data():
    """
    需提供码表，暂无，默认1000，代表S1总部
    :return:
    """
    return random.choice(["s1", "s12", "s11"])
    # return 's1'


# 个人绑定银行卡数据
def make_bind_card_data(busi_type):
    """
    个人绑定银行卡/企业绑定银行账户(仅需网络支付填写)
    :param busi_type:
    :return:
    """
    if busi_type == "01":
        bind_card = random.choice(["11", "12"])
    else:
        bind_card = ""
    return bind_card


# 关联支付账户数据
def make_pay_id_data(busi_type, act_cd):
    """
    关联支付账户数据，银行卡收单业务不填，网络和预付卡业务，取支付账户表中的支付账号
    :param busi_type:
    :param act_cd:
    :return:
    """
    if busi_type == "02":
        pay_id = ""
    else:
        pay_id = act_cd
    return pay_id


# 婚姻状态数据
def make_marriage_data(data):
    """
    25岁以下，未婚，25岁以上，已婚、离异、丧偶、其他。
    :param data: 身份证号或生日
    :return:
    """

    if len(data) <= 10:  # 生日
        ctid = int(data[:4])
    else:  # 身份证号
        ctid = int(data[6:10])
    year_now = time.strftime("%Y", time.localtime())
    age = int(year_now) - ctid
    if age < 25:
        marriage = "1"
    else:
        marriage = random.choice([
            "2",
            "3",
            "4",
            "5"
        ])
    return marriage


# 境内外标识数据
def make_bord_flag_data():
    """
    境内外标识数据,网络支付、预付卡、银行卡收单必须填写,境内、外数量比例4:1
    :return:
    """
    return random.choice(["11" if num > 1 else "12" for num in range(10)])


# 商户所在国家数据
def make_con_nation_data(bord_flag):
    """如果境内外标识为11(境内)，商户所在地选中国，为12，
    商户所在第随机选别的国家，台湾暂时放在境外，
    网络支付、预付卡、银行卡收单必须填写"""
    if bord_flag == "11":
        con_nation = "CHN"
    else:
        con_nation = random.choice([
            "ALB",  # 阿尔巴尼亚
            "DZA",  # 阿尔及利亚
            "AFG",  # 阿富汗
            "ARG",  # 阿根廷
            "ARE",  # 阿联酋
            "ABW",  # 阿鲁巴
            "OMN",  # 阿曼
            "AZE",  # 阿塞拜疆
            "EGY",  # 埃及
            "ETH",  # 埃塞俄比亚
            "IRL",  # 爱尔兰
            "EST",  # 爱沙尼亚
            "AND",  # 安道尔
            "AGO",  # 安哥拉
            "AIA",  # 安圭拉
            "ATG",  # 安提瓜和巴布达
            "AUT",  # 奥地利
            "ALA",  # 奥兰群岛
            "AUS",  # 澳大利亚
            "MAC",  # 澳门
            "BRB",  # 巴巴多斯
            "PNG",  # 巴布亚新几内亚
            "BHS",  # 巴哈马
            "PAK",  # 巴基斯坦
            "PRY",  # 巴拉圭
            "PSE",  # 巴勒斯坦
            "BHR",  # 巴林
            "PAN",  # 巴拿马
            "BRA",  # 巴西
            "BLR",  # 白俄罗斯
            "BMU",  # 百慕大
            "BGR",  # 保加利亚
            "MNP",  # 北马里亚纳群岛
            "BEN",  # 贝宁
            "BEL",  # 比利时
            "ISL",  # 冰岛
            "PRI",  # 波多黎各
            "BIH",  # 波黑
            "POL",  # 波兰
            "BOL",  # 玻利维亚
            "BLZ",  # 伯利兹
            "BWA",  # 博茨瓦纳
            "BTN",  # 不丹
            "BFA",  # 布基纳法索
            "BDI",  # 布隆迪
            "BVT",  # 布韦岛
            "PRK",  # 朝鲜
            "GNQ",  # 赤道几内亚
            "DNK",  # 丹麦
            "DEU",  # 德国
            "TLS",  # 东帝汶
            "TGO",  # 多哥
            "DOM",  # 多米尼加
            "DMA",  # 多米尼克
            "RUS",  # 俄罗斯
            "ECU",  # 厄瓜多尔
            "ERI",  # 厄立特里亚
            "FRA",  # 法国
            "FRO",  # 法罗群岛
            "PYF",  # 法属波利尼西亚
            "GUF",  # 法属圭亚那
            "ATF",  # 法属南部领地
            "MAF",  # 法属圣马丁
            "VAT",  # 梵蒂冈
            "PHL",  # 菲律宾
            "FJI",  # 斐济群岛
            "FIN",  # 芬兰
            "CPV",  # 佛得角
            "GMB",  # 冈比亚
            "COG",  # 刚果（布）
            "COD",  # 刚果（金）
            "COL",  # 哥伦比亚
            "CRI",  # 哥斯达黎加
            "GRD",  # 格林纳达
            "GRL",  # 格陵兰
            "GEO",  # 格鲁吉亚
            "GGY",  # 根西岛
            "CUB",  # 古巴
            "GLP",  # 瓜德罗普
            "GUM",  # 关岛
            "GUY",  # 圭亚那
            "KAZ",  # 哈萨克斯坦
            "HTI",  # 海地
            "KOR",  # 韩国
            "NLD",  # 荷兰
            "BES",  # 荷兰加勒比区
            "SXM",  # 荷属圣马丁
            "HMD",  # 赫德岛和麦克唐纳群岛
            "MNE",  # 黑山
            "HND",  # 洪都拉斯
            "KIR",  # 基里巴斯
            "DJI",  # 吉布提
            "KGZ",  # 吉尔吉斯斯坦
            "GIN",  # 几内亚
            "GNB",  # 几内亚比绍
            "CAN",  # 加拿大
            "GHA",  # 加纳
            "GAB",  # 加蓬
            "KHM",  # 柬埔寨
            "CZE",  # 捷克
            "ZWE",  # 津巴布韦
            "CMR",  # 喀麦隆
            "QAT",  # 卡塔尔
            "CYM",  # 开曼群岛
            "CCK",  # 科科斯群岛
            "COM",  # 科摩罗
            "CIV",  # 科特迪瓦
            "KWT",  # 科威特
            "HRV",  # 克罗地亚
            "KEN",  # 肯尼亚
            "COK",  # 库克群岛
            "CUW",  # 库拉索
            "LVA",  # 拉脱维亚
            "LSO",  # 莱索托
            "LAO",  # 老挝
            "LBN",  # 黎巴嫩
            "LTU",  # 立陶宛
            "LBR",  # 利比里亚
            "LBY",  # 利比亚
            "LIE",  # 列支敦士登
            "REU",  # 留尼汪
            "LUX",  # 卢森堡
            "RWA",  # 卢旺达
            "ROU",  # 罗马尼亚
            "MDG",  # 马达加斯加
            "IMN",  # 马恩岛
            "MDV",  # 马尔代夫
            "FLK",  # 马尔维纳斯群岛（福克兰）
            "MLT",  # 马耳他
            "MWI",  # 马拉维
            "MYS",  # 马来西亚
            "MLI",  # 马里
            "MKD",  # 马其顿
            "MHL",  # 马绍尔群岛
            "MTQ",  # 马提尼克
            "MYT",  # 马约特
            "MUS",  # 毛里求斯
            "MRT",  # 毛里塔尼亚
            "USA",  # 美国
            "UMI",  # 美国本土外小岛屿
            "ASM",  # 美属萨摩亚
            "VIR",  # 美属维尔京群岛
            "MNG",  # 蒙古国；蒙古
            "MSR",  # 蒙塞拉特岛
            "BGD",  # 孟加拉国
            "PER",  # 秘鲁
            "FSM",  # 密克罗尼西亚联邦
            "MMR",  # 缅甸
            "MDA",  # 摩尔多瓦
            "MAR",  # 摩洛哥
            "MCO",  # 摩纳哥
            "MOZ",  # 莫桑比克
            "MEX",  # 墨西哥
            "NKR",  # 纳戈尔诺-卡拉巴赫
            "NAM",  # 纳米比亚
            "ZAF",  # 南非
            "ATA",  # 南极洲
            "SGS",  # 南乔治亚岛和南桑威奇群岛
            "SSD",  # 南苏丹
            "NRU",  # 瑙鲁
            "NPL",  # 尼泊尔
            "NIC",  # 尼加拉瓜
            "NER",  # 尼日尔
            "NGA",  # 尼日利亚
            "NIU",  # 纽埃
            "NOR",  # 挪威
            "NFK",  # 诺福克岛
            "PLW",  # 帕劳
            "PCN",  # 皮特凯恩群岛
            "PRT",  # 葡萄牙
            "JPN",  # 日本
            "SWE",  # 瑞典
            "CHE",  # 瑞士
            "SLV",  # 萨尔瓦多
            "WSM",  # 萨摩亚
            "SRB",  # 塞尔维亚
            "SLE",  # 塞拉利昂
            "SEN",  # 塞内加尔
            "CYP",  # 塞浦路斯
            "SYC",  # 塞舌尔
            "SAU",  # 沙特阿拉伯
            "BLM",  # 圣巴泰勒米岛
            "CXR",  # 圣诞岛
            "STP",  # 圣多美和普林西比
            "SHN",  # 圣赫勒拿
            "KNA",  # 圣基茨和尼维斯
            "LCA",  # 圣卢西亚
            "SMR",  # 圣马力诺
            "SPM",  # 圣皮埃尔和密克隆
            "VCT",  # 圣文森特和格林纳丁斯
            "LKA",  # 斯里兰卡
            "SVK",  # 斯洛伐克
            "SVN",  # 斯洛文尼亚
            "SJM",  # 斯瓦尔巴群岛和扬马延岛
            "SWZ",  # 斯威士兰
            "SDN",  # 苏丹
            "SUR",  # 苏里南
            "SLB",  # 所罗门群岛
            "SOM",  # 索马里
            "TJK",  # 塔吉克斯坦
            "THA",  # 泰国
            "TZA",  # 坦桑尼亚
            "TON",  # 汤加
            "TCA",  # 特克斯和凯科斯群岛
            "TTO",  # 特立尼达和多巴哥
            "TUN",  # 突尼斯
            "TUV",  # 图瓦卢
            "TUR",  # 土耳其
            "TKM",  # 土库曼斯坦
            "TKL",  # 托克劳
            "WLF",  # 瓦利斯和富图纳
            "VUT",  # 瓦努阿图
            "GTM",  # 危地马拉
            "VEN",  # 委内瑞拉
            "BRN",  # 文莱
            "UGA",  # 乌干达
            "UKR",  # 乌克兰
            "URY",  # 乌拉圭
            "UZB",  # 乌兹别克斯坦
            "ESP",  # 西班牙
            "ESH",  # 西撒哈拉
            "GRC",  # 希腊
            "HKG",  # 香港
            "SGP",  # 新加坡
            "NCL",  # 新喀里多尼亚
            "NZL",  # 新西兰
            "HUN",  # 匈牙利
            "SYR",  # 叙利亚
            "JAM",  # 牙买加
            "ARM",  # 亚美尼亚
            "YEM",  # 也门
            "IRQ",  # 伊拉克
            "IRN",  # 伊朗
            "ISR",  # 以色列
            "ITA",  # 意大利
            "IND",  # 印度
            "IDN",  # 印尼
            "GBR",  # 英国
            "VGB",  # 英属维尔京群岛
            "IOT",  # 英属印度洋领地
            "JOR",  # 约旦
            "VNM",  # 越南
            "ZMB",  # 赞比亚
            "JEY",  # 泽西岛
            "TCD",  # 乍得
            "GIB",  # 直布罗陀
            "CHL",  # 智利
            "CAF",  # 中非
            "TWN"  # 中国台湾
        ])
    return con_nation


# 客户真实有效性数据
def make_reals_data():
    """
    正常为空，不正常：
    1:留存的联系地址与注册地址不一致
    2:留存联系地址不存在或者虚构
    3:留存的电话号码属于无效、空号、已停机或无法接通
    4:证件非本人、证件伪造、变造证件
    5:拒绝配合尽职调查工作
    正常数据与不正常数据比例为1:10
    :return:
    """
    return random.choice(['' if num >= 1 else str(random.randint(1, 5)) for num in range(10)])

# 股权复杂度数据
def make_complex_data():
    complex = random.choice([
        "1",  # 股权 3层以下
        "2",  # 股权 3层及以上，有商业目的
        "3"  # 股权 3层以上；或3个及以上注册地；涉及信托/不受监管的投资基金/代名人股东等；及没有明显商业目的
    ])
    return complex

# 非自然人股权可辨识度数据
def make_clear_data():
    clear = random.choice([
        "1",  # 全民集体所有制企业等结构清晰的企业
        "2",  # 公司制企业等结构相对清晰的企业
        "3",  # 公司制外资企业等结构较难识辨的企业
        "4",  # 个人独资企业、家族企业、合伙等难以尽调的企业
        "5"  # 其他风险较高股权或控制权结构（信托、代名股东等
    ])
    return clear


# 结算类型数据
def make_statement_type_data(client_tp):
    """
    结算类型数据，如果是个人，为空，商户随机选择
    :param client_tp:
    :return:
    """
    if client_tp == "1":
        statement_type = ""
    else:
        statement_type = random.choice(["0", "1"])
    return statement_type


# 主体的交易账号种类数据
def make_ctat_data(busi_type):
    if busi_type == "02":
        ctat = "@N"
    else:
        ctat = random.choice(["01", "02"])
    return ctat


# 主体的交易账号数据
def make_ctac_data(busi_type):
    if busi_type == "02":
        ctac = ""
    else:
        ctac = make_random_num(19)
    return ctac


# 关系类型数据
def make_rel_tp_data(client_tp):
    """
    个人时为B，机构为
    :param client_tp:
    :return:
    """
    if client_tp == '1':
        rel_tp = random.choice([
            "B01",  # 夫妻关系
            "B02",  # 子女
            "B03",  # 父母
            "B04",  # 其他血亲
            "B05",  # 其他姻亲
            "B06",  # 同学
            "B07",  # 朋友
            "X",  # 未说明
            "C01"  # 受益所有人
        ])
    else:
        rel_tp = random.choice([
            "A01",  # 对公客户与法人代表
            "A02",  # 对公客户与联系人
            "A03",  # 对公客户与负责人
            "A04",  # 对公客户与董事
            "A05",  # 对公客户与股东
            "A06",  # 母公司与子公司
            "A07",  # 代理
            "A08",  # 投资与被投资
            "A09",  # 其他关联单位A
            "A10",  # 企业团体
            "A11",  # 银行团体
            "A12",  # 家族企业
            "X",  # 未说明
            "C01"  # 受益所有人
        ])
    return rel_tp


# 证件签发日期数据
def make_iss_dt_data(ctid_edt):
    """
    有效期减去10,20,长期的随机减去一定年数
    :return:
    """
    year_now = time.strftime("%Y", time.localtime())
    mouth_date = time.strftime("%m%d", time.localtime())
    if ctid_edt == "99991231":
        sign_date = str(int(year_now) - random.choice([3, 5, 10, 20, 30])) + mouth_date
    else:
        date = random.choice([10, 20])
        # age = int(year_now) - int(ctid[6:10])
        sign_date = int(ctid_edt[:4]) - date
        sign_date = str(sign_date) + ctid_edt[4:]

    return sign_date


# 交易折合美元金额数据
def make_crat_u_data(crat):
    """暂定交易金额单位为人民币，汇率取7"""
    crat_u = crat / 7
    return round(crat_u, 2)


# 交易折合人民币金额数据
def make_crat_r_data(crat):
    """取交易金额"""
    crat_r = crat
    return round(crat_r, 2)
