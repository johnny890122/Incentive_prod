import get_gdoc  # 讀入Google Sheets專用
import calculate_score
import time
import os
import numpy as np
import pandas as pd
import datetime
import warnings
warnings.filterwarnings('ignore')

'''
程式碼操作方式 2021/7/30 Mic Tu
1. Input File: 將以下檔案放置至Input資料夾當中
    每月更新資料:
        (1) '站點打卡_for-attendance_{月份全名: 例如June}.xlsx'
        (2) 'IB_production_{月份全名:例如June}.xlsx'
        (3) 'OB_production_{月份全名:例如June}.xlsx'
        (4) 'INV_production_{月份全名:例如June}.xlsx'
        (5) 'WMS_label.csv'
    不用每月更新資料:
        (6) 'revise_station.xlsx'
2. 更改月份: 將第32行的month改成欲計算的月份即可，格式為"YYYY-MM"，例如"2021-06"
3. 更改連結: 將第52~75行的連結換成當月連結；原則上只有貼標每個月需要更改，若其他工作表連結有更改也須同步更動，並確認欄位是否有更動
4. 更改計算指標: 確認第77~162行至第行的內容是否需更動，各項內容的功能如下:
    (1) cat_name_checked: 不分樓層的類別資料(用於incentive_checked)，於incentive check時會用到
    (2) cat_name: 將Putaway, RT_picking, RT_putaway, Cyclecount分成三、四樓，其餘和cat_name_checked相同，於計算agent, team lead iph時會用到
    (3) type_dic: 站點打卡(key)與對應到的計算Productivity的工作種類(value)，於抓取打卡資料的時候進行轉換使用
    (4) productivity_varable: 每個工作種類計算IPH的指標，於計算agent, team lead iph時會用到
    (5) team_prod_dict: 每個工作種類(key)所屬的控場(value)種類，於計算team lead iph時會用到
'''

month = "2022-03"

month_first_day = datetime.datetime.strptime(month, "%Y-%m")
month_num = str(month_first_day.month)  # 得到str月份
month_shortname = month_first_day.strftime("%b")  # e.g. Jul, Jun
month_fullname = month_first_day.strftime("%B")  # e.g. July, June

class gdoc_information():
    def __init__(self):
        self.SCOPES = []
        self.SAMPLE_SPREADSHEET_ID = []
        self.SAMPLE_RANGE_NAME = []

    def trans(self):
        tmp = []
        tmp.extend(self.SCOPES)
        tmp.extend(self.SAMPLE_SPREADSHEET_ID)
        tmp.extend(self.SAMPLE_RANGE_NAME)
        return tmp

# Input Google Sheets: 確認以下連結是否需要更新
# new tag 貼標紀錄
tag_gdoc = gdoc_information()
tag_gdoc.SCOPES = ['https://docs.google.com/spreadsheets/d/1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8/edit']  # 每個月要改網址
tag_gdoc.SAMPLE_SPREADSHEET_ID = ['1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8']  # 網址的中間那段頁碼
tag_gdoc.SAMPLE_RANGE_NAME = ['!A2:G']  # 注意要抓取的欄位

# 收發 單獨一頁
docked_gdoc = gdoc_information()
docked_gdoc.SCOPES = ['https://docs.google.com/spreadsheets/d/1eDn98UQJuJRKN-8IaQo6MDlOMCCV4HZE2Q8iSA6oXeE/edit']  # 不用每個月更改
docked_gdoc.SAMPLE_SPREADSHEET_ID = ['1eDn98UQJuJRKN-8IaQo6MDlOMCCV4HZE2Q8iSA6oXeE']
docked_gdoc.SAMPLE_RANGE_NAME = ['!A2:K']  # 注意要抓取的欄位

# 印標紀錄
print_gdoc = gdoc_information()
print_gdoc.SCOPES = ['https://docs.google.com/spreadsheets/d/1uBRnzC3oNGKKjWt8kHRzYBj75ZDe-G9YW6wzPsR6fxY/edit']  # 不用每個月更改
print_gdoc.SAMPLE_SPREADSHEET_ID = ['1uBRnzC3oNGKKjWt8kHRzYBj75ZDe-G9YW6wzPsR6fxY']
print_gdoc.SAMPLE_RANGE_NAME = ['!A3:R']  # 注意要抓取的欄位

# 人力資料 schema
ppl_schema = gdoc_information()
ppl_schema.SCOPES = ['https://docs.google.com/spreadsheets/d/1fKqmL3VS1aDjdeJR_MqLQwu9mdEjf_Ci8PV1QCp-M6Q/edit']  # 不用每個月更改
ppl_schema.SAMPLE_SPREADSHEET_ID = ['1fKqmL3VS1aDjdeJR_MqLQwu9mdEjf_Ci8PV1QCp-M6Q']
ppl_schema.SAMPLE_RANGE_NAME = ['通訊錄']  # 抓整張工作表，之後再選要的欄位

cat_name_checked = ['Docked', 'Arrived', 'Counting', 'QC', 'Labeling',
                    'Received', 'Putaway', 'Picking', 'Packing', 'AWB', 'RTS',
                    'RT_picking', 'RT_putaway', 'Cyclecount', 'Print']  # , 'Testing']  # 新增新的種類

cat_name = ['Docked', 'Arrived', 'Counting', 'QC', 'Labeling',
            'Received', 'Putaway', 'Putaway_4floor', 'Picking', 'Packing',
            'AWB', 'RTS', 'RT_picking', 'RT_picking_4floor', 'RT_putaway',
            'RT_putaway_4floor', 'Cyclecount', 'Cyclecount_4floor', 'Print']  # , 'Testing']  # 新增新的種類


type_dic = {
    '碼頭收發': 'Docked',
    '收貨': 'Arrived',
    '進貨計數': 'Counting',
    '品管': 'QC',
    '貼標': 'Labeling',
    '貴重驗收': 'Received',
    '箱賣': 'Received',
    '小驗': 'Received',
    '大驗': 'Received',
    '上架基架': 'Putaway',
    '上架棧板': 'Putaway',
    '上架基架_四樓': 'Putaway_4floor',
    '上架棧板_四樓': 'Putaway_4floor',
    '揀貨': 'Picking',
    '包裝': 'Packing',
    '出貨': 'AWB',
    '退貨出貨': 'RTS',
    '退貨包裝': 'RTS',
    '退貨揀貨': 'RTS',
    '移庫揀貨': 'RT_picking',
    '移庫上架': 'RT_putaway',
    '移庫揀貨_四樓': 'RT_picking_4floor',
    '移庫上架_四樓': 'RT_putaway_4floor',
    '盤點系統盤': 'Cyclecount',
    '盤點系統盤_四樓': 'Cyclecount_4floor',
    '印標': 'Print'
    # '出貨5S': 'Testing'  # 直接加上新的種類即可
}

productivity_varable = {
    'DL%': 1,
    'DL % threshold': 0.6,
    'Docked': 75,
    'Arrived': 125,
    'QC': 4638,
    'Labeling': 850,
    'Received': 800,
    'Putaway': 65,
    'Putaway_4floor': 65,
    'Picking': 114,
    'Packing': 143,
    'Counting': 1000,
    'AWB': 720,
    'RTS': 300,
    'RT_picking': 726,
    'RT_putaway': 726,
    'RT_picking_4floor': 726,
    'RT_putaway_4floor': 726,
    'Cyclecount': 850,
    'Cyclecount_4floor': 850,
    'Print': 200  # 20210716待確認
    # 'Testing': 20  # 新增計算IPH指標
}

team_prod_dict = {
    'Picking': '出貨控場',
    'Packing': '出貨控場',
    'AWB': '出貨控場',
    'Arrived': '進貨控場',
    'Counting': '進貨控場',
    'QC': '進貨控場',
    'Labeling': '進貨控場',
    'Received': '進貨控場',
    'Docked': '進貨控場',
    'Print': '進貨控場',
    'RT_picking': '移庫控場',
    'RT_putaway': '移庫控場',
    'RT_picking_4floor': '移庫控場_四樓',
    'RT_putaway_4floor': '移庫控場_四樓',
    'RTS': np.nan,
    'Putaway': '移庫控場',
    'Putaway_4floor': '移庫控場_四樓',
    'Cyclecount': '盤點控場',
    'Cyclecount_4floor': '盤點控場_四樓'
    # 'Testing': '測試控場'  # 新增種類的控場
}

# Input Files
punch_file_name = 'Input/punch_for-attendance_{}.xlsx'.format(month_fullname)
revise_station_name = 'Input/revise_station.xlsx'
inb_pics_file_path = 'Input/IB_production_{}.xlsx'.format(month_fullname)
inb_pics_file_path_new = 'Input/IB_production_{}_new.xlsx'.format(month_fullname)  # IB_production增加印標、收發、貼標後會儲存在此，並做為之後計算的input
ob_pics_file_path = 'Input/OB_production_{}.xlsx'.format(month_fullname)
inv_pics_file_path = 'Input/INV_production_{}.xlsx'.format(month_fullname)
wms_label = 'Input/WMS_label.csv'

# Output Files
tl_output_path = "Output/productivity_TL_{}.xlsx".format(month_shortname)
agent_output_path = "Output/productivity_agent_{}.xlsx".format(month_shortname)
tl_valid_output_path = "Output/productivity_TL_{}_valid.xlsx".format(month_shortname)
agent_valid_output_path = "Output/productivity_agent_{}_valid.xlsx".format(month_shortname)

# Checkpoint 1: 匯入打卡資料並進行前處理
def read_punch_file(path, revise_station_name, type_dic):
    '''
    讀入站點打卡_for-attendance資料，進行整理
    ----------------
    Input:
    1. path: 站點打卡路徑(punch_file_name)
    2. revise_station_name: 要將站點進行參照的表格
    2. type_dic: 字典，用於將站點打卡的中文站點轉換為英文
    '''
    punch_station = pd.read_excel(revise_station_name)  # 參照revise_station的工作表
    punch_station['lookup'] = punch_station['Unnamed: 1']\
        .str.cat(punch_station['function_name'], sep=', ')\
        .str.cat(punch_station['function_role'], sep=', ')  # 將Unnamed, function_name, function_role三個欄位合再一起，作為參照

    punch_raw_df = pd.read_excel(path)
    punch_raw_df = (punch_raw_df[~pd.isnull(punch_raw_df['name'])])  # 只保留有名字的打卡記錄
    punch_raw_df.drop_duplicates(inplace=True)  # 移除重複項目

    punch_raw_df['ID'] = punch_raw_df['ID'].str.lower()  # 將打卡員編轉為小寫，以利後續參照
    punch_raw_df['type'] = punch_raw_df['function'].map(type_dic)  # 新增type，為type_dic的工作種類
    punch_raw_df['type'] = punch_raw_df['type'].astype('str').replace('nan', np.nan)  # 將類別轉為字串格式，缺失值(不算Productivity的項目)為np.nan
    punch_raw_df['hour'] = punch_raw_df['min'] / 60  # 新增小時欄位

    punch_raw_df['lookup'] = punch_raw_df['Unnamed: 7']\
        .str.cat(punch_raw_df['function_name'], sep=', ')\
        .str.cat(punch_raw_df['function_role'], sep=', ')  # 將Unnamed, function_name, function_role三個欄位合再一起，作為參照
    punch_raw_df.rename(columns={'date': 'create_date', 'ID': 'operator'})
    punch_raw_df = punch_raw_df.merge(punch_station[['lookup', 'revised station']], on='lookup')\
                               .drop('lookup', axis=1)  # 參照完就把參照欄位lookup丟棄
    punch_raw_df.sort_values('created_time', inplace=True)  # 之後merge_asof需要排序
    punch_raw_df.reset_index(drop=True, inplace=True)
    return punch_raw_df


# Checkpoint 2: 匯入人力資料並進行前處理
def read_human_data():
    '''
    抓取「人力資料_schema」資料，並轉成後續需要的字典
    1. name_id_dic: 姓名(key)與員編(value)
    2. id_name_dic: 員編(key)與姓名(value)
    3. pda_name_dic: PDA帳號(key)與姓名(value)
    4. pda_id_dic: PDA帳號(key)與員編(value)
    '''
    # human_df = pd.DataFrame(get_gdoc.get_google_sheet(*ppl_schema.trans()))[[0, 1, 2, 3]]  # 只取前四欄
    human_df = pd.read_csv("tmp_input/人力資料 schema - 通訊錄.csv", usecols=["WMS帳號", "公司", "PDA帳號", "worker_name"])
    human_df.columns = ['員編', '公司', 'PDA帳號', 'worker_name']

    human_df = human_df[1:]  # 去掉第一行表頭
    id_name_dic = {str(x).lower(): y for x, y in zip(human_df['員編'], human_df['worker_name'])}
    name_id_dic = {}
    for key, value in id_name_dic.items():
        if value not in name_id_dic.keys():
            name_id_dic[value] = key
    pda_name_dic = {str(x): y for x, y in zip(human_df['PDA帳號'], human_df['worker_name'])}
    pda_id_dic = {str(x): str(y).lower() for x, y in zip(human_df['員編'], human_df['PDA帳號'])}
    return name_id_dic, id_name_dic, pda_name_dic, pda_id_dic


# Checkpoint 3: 將IB_production新增貼標、收發、印標資料
def add_data_in_inb():
    '''
    1. 新增貼標到 inb_pics_file_path (IB_production) (2021/05)
    2. 新增收發到 inb_pics_file_path (IB_production) (2021/05)
    3. 新增印標到 inb_pics_file_path (IB_production) (2021/07 新增)
    output: 更新inb_pics_file_path
    '''
    # 3-1 抓Google Sheet「人力資料schema」，存為ppl_schema_df(DataFrame)
    # ppl_schema_df = pd.DataFrame(get_gdoc.get_google_sheet(*ppl_schema.trans()))[[0, 2]]  # 只選員編與貼標ID
    ppl_schema_df = pd.read_csv("tmp_input/人力資料 schema - 通訊錄.csv", usecols=["WMS帳號", "PDA帳號"])
    ppl_schema_df = ppl_schema_df[1:]  # 去掉第一行表頭
    ppl_schema_df.columns = ['員編', '貼標ID']
    time3_1 = time.time()
    print('Checkpoint 3-1 人力資料_schema SUCCEED    Spend {:.2f} seconds'.format(time3_1 - time2))

    # 3-2 抓取貼標資料，在get_gdoc.get_tag_data中匯出excel，並存為tag_summary
    tag_month_path = 'Output/label_raw_{}.csv'.format(month_shortname)
    if os.path.exists(tag_month_path):  # 如果有檔案，直接讀取過去檔案
        tag_df = pd.read_csv(tag_month_path)
    else:  # 如果沒有檔案，執行get_gdoc.get_tag_data取得資料
        tag_df = get_gdoc.get_tag_data(month_first_day, tag_month_path, *tag_gdoc.trans())
    tag_df = tag_df[["版標流水號", "貼標開始", "貼標人數(人)", "貼標ID"]]

    wms_label_df = pd.read_csv(wms_label)[['tracking_id', 'batch_qty']]  # 用得抓取每個流水號每個batch有多少數量
    tag_df = pd.merge(tag_df, wms_label_df, left_on='版標流水號', right_on='tracking_id')  # 將每個貼標有多少個batch結合
    tag_df["貼標ID"] = tag_df["貼標ID"].astype('str')
    tag_df['貼標開始'] = pd.to_datetime(tag_df['貼標開始'], errors='coerce')
    tag_df['員工作業PCS'] = tag_df['batch_qty'] / tag_df['貼標人數(人)']
    tag_summary = tag_df.groupby(['貼標開始', '貼標ID']).sum()
    tag_summary = tag_summary.reset_index()
    tag_summary = tag_summary.merge(ppl_schema_df, left_on='貼標ID', right_on='貼標ID', how='left')  # 得到貼標的員編
    tag_summary = tag_summary[tag_summary['員編'].notnull()]
    tag_summary['type'] = 'Labeling'
    tag_summary['box'] = 0  # 其他種類才用到box，貼標資料皆為0
    tag_summary['orders'] = 0  # 其他種類才用到orders，貼標資料皆為0
    tag_summary = tag_summary[['員編', 'type', '員工作業PCS', 'box', 'orders', '貼標開始']]
    tag_summary.columns = ['operator', 'type', 'total_pcs', 'box', 'orders', 'inbound_date']  # 合併資料統一要這幾個欄位
    tag_summary['inbound_date'] = pd.to_datetime(tag_summary['inbound_date'], errors='coerce')
    time3_2 = time.time()
    print('Checkpoint 3-2 tag_summary SUCCEED        Spend {:.2f} seconds'.format(time3_2 - time3_1))

    # 3-3 抓取新增收發，並匯出excel，並存為docked_summary
    docked_path = 'docked_raw_{}.xlsx'.format(month_shortname)
    # docked_df = pd.DataFrame(get_gdoc.get_google_sheet(*docked_gdoc.trans()))
    docked_df = pd.read_csv("tmp_input/Incentive收發 - Sheet1.csv")
    docked_df.to_excel(docked_path, index=False)

    docked_df.columns = ['員編', 'INbound ID', '國碼', '是否拒收', '狀態', '備註', 'Cancel後新單', 'QTY', '收發時間', 'DATE', 'HOUR']
    docked_df['Month'] = pd.to_datetime(docked_df['DATE'], errors='coerce').dt.month.astype(float)
    docked_df = docked_df[docked_df['Month'] == int(month_num)]
    docked_df['員編'] = docked_df['員編'].astype('str')
    docked_df['員編'] = docked_df['員編'].apply(lambda x: 'remove_' if len(x) != 5 else x)
    docked_df = docked_df[docked_df['員編'] != 'remove_']
    docked_df['收發時間'] = pd.to_datetime(docked_df['收發時間'], errors='coerce')
    docked_summary = docked_df.groupby(['收發時間', '員編'])['INbound ID'].count()
    docked_summary = docked_summary.reset_index()

    # Mapping 人力資料 schema 五碼變SP
    docked_summary = docked_summary.rename(columns={"員編": "五碼"})
    docked_summary = docked_summary.merge(ppl_schema_df, left_on='五碼', right_on='貼標ID', how='left')
    docked_summary['type'] = 'Docked'
    docked_summary['box'] = 0  # 其他種類才用到box，收發資料皆為0
    docked_summary['total_pcs'] = 0  # 其他種類才用到orders，收發資料皆為0
    docked_summary = docked_summary[['員編', 'type', 'total_pcs', 'box', 'INbound ID', '收發時間']]
    docked_summary.columns = ['operator', 'type', 'total_pcs', 'box', 'orders', 'inbound_date']  # 合併資料統一要這幾個欄位
    print(docked_summary.head())
    time3_3 = time.time()
    print('Checkpoint 3-3 docked_summary SUCCEED     Spend {:.2f} seconds'.format(time3_3 - time3_2))

    # 檔案4. print_summary: 如果有檔案，直接讀取過去檔案；反之則執行processing.take_month_data取得資料
    print_month_path = 'print_raw_{}.xlsx'.format(month_shortname)
    print_df = get_gdoc.get_print_data(month_first_day, print_month_path, *print_gdoc.trans())

    print_df = print_df[["印標人員", "DATE"]]
    print_df['印標人員'] = print_df['印標人員'].astype('str')

    print_summary = print_df.merge(ppl_schema_df, left_on='印標人員', right_on='貼標ID', how='left')
    print_summary = print_summary[print_summary['員編'].notnull()]
    print_summary['type'] = 'Print'
    print_summary['box'] = 0  # 其他種類才用到box，印標資料皆為0
    print_summary['total_pcs'] = 0  # 其他種類才用到orders，印標資料皆為0
    print_summary['orders'] = 1  # 每個orders = 1
    print_summary = print_summary[['員編', 'type', 'total_pcs', 'box', 'orders', 'DATE']]
    print_summary.columns = ['operator', 'type', 'total_pcs', 'box', 'orders', 'inbound_date']  # 合併資料統一要這幾個欄位
    print(print_summary.head(5))
    time3_4 = time.time()
    print('Checkpoint 3-4 print_df SUCCEED           Spend {:.2f} seconds'.format(time3_4 - time3_3))

    ib_df = pd.read_excel(inb_pics_file_path)
    ib_df = ib_df.append(tag_summary)
    ib_df = ib_df.append(docked_summary)
    ib_df = ib_df.append(print_summary)
    ib_df.to_excel(inb_pics_file_path_new, index=False)
    time3_5 = time.time()
    print('Checkpoint 3-5 add to excel SUCCEED       Spend {:.2f} seconds'.format(time3_5 - time3_4))


# Checkpoint 4: 輸入資料格式統一
# Checkpoint 4-1: IB_production
def read_ibs(inb_pics_file_path_new, id_name_dic):
    '''
    read inbound PICS 的資料 (excel)
    input:
    1. inb_pics_file_path_new
    2. id_name_dic: 名字對應到 id
    '''
    inb_pic_df = pd.read_excel(inb_pics_file_path_new, parse_dates=['inbound_date'])
    inb_pic_df = inb_pic_df.rename(columns={'inbound_date': 'create_date'})
    inb_pic_df = inb_pic_df[inb_pic_df['operator'].notnull()]  # 排除 operator 為空的列
    inb_pic_df['operator'] = inb_pic_df['operator'].str.lower()  # 員編轉小寫
    inb_pic_df['name'] = inb_pic_df['operator'].map(id_name_dic)  # 利用 id 轉名字
    inb_pic_df = inb_pic_df[['name', 'operator', 'type', 'create_date', 'total_pcs', 'box', 'orders']]
    inb_pic_df = inb_pic_df.rename(columns={'total_pcs': 'pcs', 'create_date': 'create_time'})
    return inb_pic_df[['name', 'operator', 'type', 'create_time', 'pcs', 'box', 'orders']]


# Checkpoint 4-2: OB_production
def read_obs(ob_pics_file_path, id_name_dic, pda_id_dic):
    '''
    read oubound / inv PICS 的資料 (excel)
    input:
    1. path_name : PICS 資料連結
    2. name_id_dic: 名字對應到 id
    read oubound / inv PICS 的資料 (csv)
    因為資料欄位名稱不一樣，所以才要分開讀
    '''
    ob_pic_df = pd.read_excel(ob_pics_file_path, parse_dates=['create_time'])
    ob_pic_df['workers'] = ob_pic_df['workers'].str.lower().astype('str')
    ob_pic_df['type'] = ob_pic_df['type'].map({'1_picking': 'Picking', '3_packing': 'Packing', '4_awb': 'AWB'})

    def get_operator(worker):
        if 'sp' not in worker and worker in pda_id_dic:
            return pda_id_dic[worker]
        else:
            return worker
    ob_pic_df['operator'] = ob_pic_df['workers'].apply(get_operator)  # 如果workers是員編就輸出員編，是PDA帳號就轉成員編
    ob_pic_df['name'] = ob_pic_df['operator'].map(id_name_dic)
    ob_pic_df['box'] = 0
    ob_pic_df['orders'] = 0
    return ob_pic_df[['name', 'operator', 'type', 'create_time', 'pcs', 'box', 'orders']]


# Checkpoint 4-3: INV_production
def read_inv(inv_pics_file_path, id_name_dic):
    '''
    read oubound / inv PICS 的資料 (excel)
    input:
    1. path_name: PICS 資料連結
    2. name_id_dic: 名字對應到 id
    read oubound / inv PICS 的資料 (csv)
    因為資料欄位名稱不一樣，所以才要分開讀
    '''
    inv_pic_df = pd.read_excel(inv_pics_file_path, parse_dates=['create_date'])
    inv_pic_df = inv_pic_df[inv_pic_df['operator'].notnull()]  # 排除 operator 為空的列
    inv_pic_df['operator'] = inv_pic_df['operator'].str.lower()  # 員編轉小寫
    inv_pic_df['type'] = np.where(inv_pic_df['type'] == 'Cycle_count', 'Cyclecount', inv_pic_df['type'])  # type 字串轉換
    inv_pic_df['name'] = inv_pic_df['operator'].map(id_name_dic)  # 利用 id 轉名字
    inv_pic_df['box'] = 0
    inv_pic_df['orders'] = 0
    inv_pic_df = inv_pic_df.rename(columns={'create_date': 'create_time'})
    return inv_pic_df[['name', 'operator', 'type', 'create_time', 'pcs', 'box', 'orders']]


# Checkpoint 4-4: 將IB_production、OB_production、INV_production資料合併，得到whole_df
def get_whole_df(ib_df, inv_df, ob_df):
    '''
    將ib_df、inv_df、ob_df合併
    input: ib_df, inv_df, ob_df
    output: 合併後的資料whole_df
    '''
    whole_df = pd.concat([ib_df, inv_df, ob_df])
    whole_df['create_time'] = pd.to_datetime(whole_df['create_time'], errors='coerce')  # 轉不了日期就跳過

    whole_df.dropna(how='any', inplace=True)
    whole_df = whole_df[whole_df['create_time'].dt.date != datetime.date(1899, 12, 30)]
    whole_df.sort_values(['create_time'], inplace=True)
    # 'total_pcs'直接列出之後計算IPH的Productivity，'Arrived', 'Docked' 使用orders計算，'Putaway'使用box計算，其他皆使用pcs計算
    whole_df['total_pcs'] = np.where(
        whole_df['type'].isin(['Arrived', 'Docked', 'Print']), whole_df['orders'],
        np.where(whole_df['type'] == 'Putaway', whole_df['box'], whole_df['pcs']))
    return whole_df


# Checkpoint 5-1: 將whole_df、punch_df合併，得到merge_df(位於calculate_score.py)
# Checkpoint 5-2: 將merge_df依各種工作種類合併(位於calculate_score.py)
# Checkpoint 6: 「未經過 valid」計算productivity_agent(位於calculate_score.py)
# Checkpoint 7: 「未經過 valid」計算productivity_TL(位於calculate_score.py)

# Checkpoint 8: 將merge_df進行validation，產出 valid_whole_df
# Checkpoint 9: 「經過 valid」計算productivity_agent(位於calculate_score.py)
# Checkpoint 10: 「經過 valid」計算productivity_TL(位於calculate_score.py)

if __name__ == '__main__':
    time0 = time.time()
    punch_df = read_punch_file(punch_file_name, revise_station_name, type_dic)
    punch_df.to_csv('punch_df.csv', index=False, encoding="utf_8_sig")
    punch_df.dropna(subset=['created_time', 'end_time'], axis=0, inplace=True)

    time1 = time.time()
    print('Checkpoint 1 read_punch_file SUCCEED      Spend {:.2f} seconds'.format(time1 - time0))

    name_id_dic, id_name_dic, pda_name_dic, pda_id_dic = read_human_data()
    time2 = time.time()
    print('Checkpoint 2 read_human_datas SUCCEED     Spend {:.2f} seconds'.format(time2 - time1))

    add_data_in_inb()
    time3 = time.time()
    print('Checkpoint 3 add_data_in_inb SUCCEED      Spend {:.2f} seconds'.format(time3 - time2))

    ib_df = read_ibs(inb_pics_file_path_new, id_name_dic)
    time4_1 = time.time()
    print('Checkpoint 4-1 ib_df SUCCEED              Spend {:.2f} seconds'.format(time4_1 - time3))

    ob_df = read_obs(ob_pics_file_path, id_name_dic, pda_id_dic)
    time4_2 = time.time()
    print('Checkpoint 4-2 ob_df SUCCEED              Spend {:.2f} seconds'.format(time4_2 - time4_1))

    inv_df = read_inv(inv_pics_file_path, id_name_dic)
    time4_3 = time.time()
    print('Checkpoint 4-3 inv_df SUCCEED             Spend {:.2f} seconds'.format(time4_3 - time4_2))

    whole_df = get_whole_df(ib_df, inv_df, ob_df)
    whole_df.to_csv('whole_df.csv', index=False, encoding="utf_8_sig")
    time4_4 = time.time()
    print('Checkpoint 4-4 whole_df SUCCEED           Spend {:.2f} seconds'.format(time4_4 - time4_3))

    time4 = time.time()
    print('Checkpoint 4 whole_df SUCCEED             Spend {:.2f} seconds'.format(time4 - time3))

    merge_df = calculate_score.get_merge_df(whole_df, punch_df)
    merge_df.to_csv("merge_df.csv", index=False, encoding="utf_8_sig")

    time5_1 = time.time()
    print('Checkpoint 5-1 get_merge_df SUCCEED       Spend {:.2f} seconds'.format(time5_1 - time4))
    calculate_score.get_valid_csv(merge_df, cat_name_checked)
    time5_2 = time.time()
    print('Checkpoint 5-2 get_valid_csv SUCCEED      Spend {:.2f} seconds'.format(time5_2 - time5_1))

    time5 = time.time()
    print('Checkpoint 5 SUCCEED   Spend {:.2f} seconds'.format(time5 - time4))

    calculate_score.get_prod_agent_score(cat_name, productivity_varable, whole_df, punch_df, agent_output_path)
    time6 = time.time()
    print('Checkpoint 6 productivity_agent SUCCEED   Spend {:.2f} seconds'.format(time6 - time5))

    calculate_score.get_prod_TL_score(productivity_varable, team_prod_dict, whole_df, punch_df, tl_output_path)
    time7 = time.time()
    print('Checkpoint 7 productivity_TL SUCCEED      Spend {:.2f} seconds'.format(time7 - time6))

    valid_whole_df = calculate_score.get_valid_whole_df(merge_df)
    valid_whole_df.dropna(axis=0, inplace=True)
    time8 = time.time()
    print('Checkpoint 8 get_valid_whole_df SUCCEED      Spend {:.2f} seconds'.format(time8 - time7))

    calculate_score.get_prod_agent_score(cat_name, productivity_varable, valid_whole_df, punch_df, agent_valid_output_path)
    time9 = time.time()
    print('Checkpoint 9 productivity_valid_agent SUCCEED     Spend {:.2f} seconds'.format(time9 - time8))

    calculate_score.get_prod_TL_score(productivity_varable, team_prod_dict, valid_whole_df, punch_df, tl_valid_output_path)
    time10 = time.time()
    print('Checkpoint 10 productivity_valid_TL SUCCEED        Spend {:.2f} seconds'.format(time10 - time9))

    print('計算完成 共花費{:.2f}秒'.format(time10 - time0))
