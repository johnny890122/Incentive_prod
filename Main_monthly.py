#!/usr/bin/env python
# coding: utf-8

# In[1]:


import time
import os
import numpy as np
import pandas as pd
import datetime
from datetime import timedelta
import warnings
from google.oauth2.service_account import Credentials
import gspread
warnings.filterwarnings('ignore')


# In[2]:


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


# In[3]:


class gdoc_information():
    def __init__(self):
        self.SCOPES = ""
        self.SAMPLE_SPREADSHEET_ID = ""
        self.SAMPLE_RANGE_NAME = ""


# In[4]:


ppl_schema = gdoc_information()
ppl_schema.SCOPES = 'https://docs.google.com/spreadsheets/d/1fKqmL3VS1aDjdeJR_MqLQwu9mdEjf_Ci8PV1QCp-M6Q'  # 不用每個月更改
ppl_schema.SAMPLE_RANGE_NAME = '通訊錄'  # 抓整張工作表，之後再選要的欄位

tag_gdoc = gdoc_information()
tag_gdoc.SCOPES = 'https://docs.google.com/spreadsheets/d/1GUvKT8BxFsHLwgM2Jptmkpc0pIZMetoVtIUIet5UxB8/edit'  # 每個月要改網址

docked_gdoc = gdoc_information()
docked_gdoc.SCOPES = 'https://docs.google.com/spreadsheets/d/1eDn98UQJuJRKN-8IaQo6MDlOMCCV4HZE2Q8iSA6oXeE/edit'  # 不用每個月更改
docked_gdoc.SAMPLE_RANGE_NAME = "Sheet1"

print_gdoc = gdoc_information()
print_gdoc.SCOPES = 'https://docs.google.com/spreadsheets/d/1uBRnzC3oNGKKjWt8kHRzYBj75ZDe-G9YW6wzPsR6fxY/edit'  # 不用每個月更改


# In[5]:


scope = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
gs = gspread.authorize(creds)


# In[6]:


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
    punch_station['lookup'] = punch_station['Unnamed: 1']        .str.cat(punch_station['function_name'], sep=', ')        .str.cat(punch_station['function_role'], sep=', ')  # 將Unnamed, function_name, function_role三個欄位合再一起，作為參照

    punch_raw_df = pd.read_excel(path)
    punch_raw_df = (punch_raw_df[~pd.isnull(punch_raw_df['name'])])  # 只保留有名字的打卡記錄
    punch_raw_df = punch_raw_df[(punch_raw_df["date"] >= start_day) & (punch_raw_df["date"] - timedelta(days = 1) <= end_day)]
    punch_raw_df.drop_duplicates(inplace=True)  # 移除重複項目
    
    punch_raw_df['ID'] = punch_raw_df['ID'].str.lower()  # 將打卡員編轉為小寫，以利後續參照
    punch_raw_df['type'] = punch_raw_df['function'].map(type_dic)  # 新增type，為type_dic的工作種類
    punch_raw_df['type'] = punch_raw_df['type'].astype('str').replace('nan', np.nan)  # 將類別轉為字串格式，缺失值(不算Productivity的項目)為np.nan
    punch_raw_df['hour'] = punch_raw_df['min'] / 60  # 新增小時欄位

    punch_raw_df['lookup'] = punch_raw_df['Unnamed: 7']        .str.cat(punch_raw_df['function_name'], sep=', ')        .str.cat(punch_raw_df['function_role'], sep=', ')  # 將Unnamed, function_name, function_role三個欄位合再一起，作為參照
    punch_raw_df.rename(columns={'date': 'create_date', 'ID': 'operator'})
    punch_raw_df = punch_raw_df.merge(punch_station[['lookup', 'revised station']], on='lookup')                               .drop('lookup', axis=1)  # 參照完就把參照欄位lookup丟棄
    punch_raw_df.sort_values('created_time', inplace=True)  # 之後merge_asof需要排序
    punch_raw_df.reset_index(drop=True, inplace=True)
    return punch_raw_df


# In[7]:


# Checkpoint 2: 匯入人力資料並進行前處理
def read_human_data():
    '''
    抓取「人力資料_schema」資料，並轉成後續需要的字典
    1. name_id_dic: 姓名(key)與員編(value)
    2. id_name_dic: 員編(key)與姓名(value)
    3. pda_name_dic: PDA帳號(key)與姓名(value)
    4. pda_id_dic: PDA帳號(key)與員編(value)
    '''
    
    human_gsheet = gs.open_by_url(ppl_schema.SCOPES).worksheet(ppl_schema.SAMPLE_RANGE_NAME)
    human_df = pd.DataFrame(human_gsheet.get_all_records(), columns=["WMS帳號", "公司", "PDA帳號", "worker_name"])
    human_df.columns = ['員編', '公司', 'PDA帳號', 'worker_name']
    
    id_name_dic = {str(x).lower(): y for x, y in zip(human_df['員編'], human_df['worker_name'])}
    name_id_dic = {}
    for key, value in id_name_dic.items():
        if value not in name_id_dic.keys():
            name_id_dic[value] = key
    pda_name_dic = {str(x): y for x, y in zip(human_df['PDA帳號'], human_df['worker_name'])}
    pda_id_dic = {str(x): str(y).lower() for x, y in zip(human_df['員編'], human_df['PDA帳號'])}
    return name_id_dic, id_name_dic, pda_name_dic, pda_id_dic


# In[8]:


# Checkpoint 3: 將IB_production新增貼標、收發、印標資料
def add_data_in_inb(time2):
#     '''
#     1. 新增貼標到 inb_pics_file_path (IB_production) (2021/05)
#     2. 新增收發到 inb_pics_file_path (IB_production) (2021/05)
#     3. 新增印標到 inb_pics_file_path (IB_production) (2021/07 新增)
#     output: 更新inb_pics_file_path
#     '''
    # 3-1 抓Google Sheet「人力資料schema」，存為ppl_schema_df(DataFrame)
    ppl_schema_gsheet = gs.open_by_url(ppl_schema.SCOPES).worksheet(ppl_schema.SAMPLE_RANGE_NAME)
    ppl_schema_df = pd.DataFrame(ppl_schema_gsheet.get_all_records(), columns=["WMS帳號", "PDA帳號"])
    ppl_schema_df.columns = ['員編', '貼標ID']
    ppl_schema_df['貼標ID'] = ppl_schema_df['貼標ID'].astype("str")
    ppl_schema_df.dropna(inplace=True)
    time3_1 = time.time()
    print('Checkpoint 3-1 人力資料_schema SUCCEED    Spend {:.2f} seconds'.format(time3_1 - time2))

#     # 3-2 抓取貼標資料，在get_gdoc.get_tag_data中匯出excel，並存為tag_summary
    
    tag_df = pd.DataFrame()
    tag_gsheet = gs.open_by_url(tag_gdoc.SCOPES)
    
    for day in pd.date_range(start=start_day,end=end_day):
        df = get_everyday_tag_data(day.strftime("%Y-%m-%d"), tag_gsheet)
        tag_df = pd.concat([tag_df, df])
        time.sleep(3)
    
    tag_df = tag_df[["版標流水號", "貼標開始", "貼標人數(人)", "貼標ID"]]
    
    wms_label_df = pd.read_csv(wms_label).rename(columns={"_col0": "date"}) 
    wms_label_df["date"] = pd.to_datetime(wms_label_df["date"], errors='coerce')
    wms_label_df = wms_label_df[(wms_label_df["date"] >= start_day) & (wms_label_df["date"] - timedelta(days = 1) <= end_day)][['tracking_id', 'batch_qty']] # 抓取每個流水號每個batch有多少數量

    tag_df = pd.merge(tag_df, wms_label_df, left_on='版標流水號', right_on='tracking_id') # 將每個貼標有多少個batch結合
    tag_df["貼標ID"] = tag_df["貼標ID"].astype('str')
    tag_df['貼標人數(人)'] = tag_df['貼標人數(人)'].astype("int")
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
    print(tag_summary.head())
    time3_2 = time.time()
    print('Checkpoint 3-2 tag_summary SUCCEED        Spend {:.2f} seconds'.format(time3_2 - time3_1))
    
    # 3-3 抓取新增收發，並匯出excel，並存為docked_summary
    docked_gsheet = gs.open_by_url(docked_gdoc.SCOPES).worksheet(docked_gdoc.SAMPLE_RANGE_NAME)
    docked_df = pd.DataFrame(docked_gsheet.get_all_records())
    docked_df['收發時間'] = pd.to_datetime(docked_df['收發時間'], errors='coerce')
    docked_df.dropna(subset=["收發時間"], axis=0, inplace=True)
    docked_df["DATE"] = docked_df["收發時間"].apply(lambda x: x.strftime('%Y-%m-%d'))
    docked_df["HOUR"] = docked_df["收發時間"].apply(lambda x: x.hour)
    
    docked_df = docked_df[(docked_df["收發時間"] >= start_day) & (docked_df["收發時間"] - timedelta(days = 1) <= end_day)]
    
    docked_path = 'tmp_output/docked_raw_{}.xlsx'.format(month)
    docked_df.to_excel(docked_path, index=False)

    docked_df.columns = ['員編', 'INbound ID', '國碼', '是否拒收', '狀態', '備註', 'Cancel後新單', 'QTY', '收發時間', 'DATE', 'HOUR']
    docked_df['員編'] = docked_df['員編'].astype('str')
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
    print_df = pd.DataFrame()
    print_gsheet = gs.open_by_url(print_gdoc.SCOPES)
    for day in pd.date_range(start=start_day,end=end_day):
        df = get_everyday_print_data(day.strftime("%Y-%m-%d"), print_gsheet)
        print_df = pd.concat([print_df, df])
        time.sleep(3)

    print_df = print_df[["印標人員", "DATE"]]
    print_df['印標人員'] = print_df['印標人員'].apply(lambda x : str(x).replace("x", "0").replace("X", "0"))
    print_df['印標人員'] = print_df['印標人員'].astype("str")
    print_summary = print_df.merge(ppl_schema_df, left_on='印標人員', right_on='貼標ID', how='left')
    print_summary = print_summary[print_summary['員編'].notnull()]
    print_summary['type'] = 'Print'
    print_summary['box'] = 0  # 其他種類才用到box，印標資料皆為0
    print_summary['total_pcs'] = 0  # 其他種類才用到orders，印標資料皆為0
    print_summary['orders'] = 1  # 每個orders = 1
    print_summary = print_summary[['員編', 'type', 'total_pcs', 'box', 'orders', 'DATE']]
    print_summary.columns = ['operator', 'type', 'total_pcs', 'box', 'orders', 'inbound_date']  # 合併資料統一要這幾個欄位
    print(print_summary.head())
    time3_4 = time.time()
    print('Checkpoint 3-4 print_df SUCCEED           Spend {:.2f} seconds'.format(time3_4 - time3_3))

    ib_df = pd.read_excel(inb_pics_file_path)
    ib_df = ib_df[(ib_df["inbound_date"] >= start_day) & (ib_df["inbound_date"] - timedelta(days = 1) <= end_day)]

    ib_df = ib_df.append(tag_summary)
    ib_df = ib_df.append(docked_summary)
    ib_df = ib_df.append(print_summary)

    ib_df.to_excel(inb_pics_file_path_new, index=False)
    time3_5 = time.time()
    print('Checkpoint 3-5 add to excel SUCCEED       Spend {:.2f} seconds'.format(time3_5 - time3_4))
    


# In[9]:


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


# In[10]:


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
    ob_pic_df = ob_pic_df[(ob_pic_df["create_time"] >= start_day) & (ob_pic_df["create_time"] - timedelta(days = 1) <= end_day)]
    
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


# In[11]:


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
    inv_pic_df = inv_pic_df[(inv_pic_df["create_date"] >= start_day) & (inv_pic_df["create_date"] - timedelta(days = 1) <= end_day)]
    
    inv_pic_df = inv_pic_df[inv_pic_df['operator'].notnull()]  # 排除 operator 為空的列
    inv_pic_df['operator'] = inv_pic_df['operator'].str.lower()  # 員編轉小寫
    inv_pic_df['type'] = np.where(inv_pic_df['type'] == 'Cycle_count', 'Cyclecount', inv_pic_df['type'])  # type 字串轉換
    inv_pic_df['name'] = inv_pic_df['operator'].map(id_name_dic)  # 利用 id 轉名字
    inv_pic_df['box'] = 0
    inv_pic_df['orders'] = 0
    inv_pic_df = inv_pic_df.rename(columns={'create_date': 'create_time'})
    return inv_pic_df[['name', 'operator', 'type', 'create_time', 'pcs', 'box', 'orders']]


# In[12]:


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


# In[13]:


# Checkpoint 5-1: 將whole_df、punch_df合併，得到merge_df
def get_merge_df(whole_df, punch_df):
    '''
    將whole_df、punch_df合併，並判斷whole_df的create time是否在punch_df打卡的時段
    input: whole_df, punch_df
    output: merge_df
    '''
    
    whole_df.sort_values('create_time', inplace=True)
    punch_df.sort_values('created_time', inplace=True)

    merge_df = pd.merge_asof(
        whole_df, punch_df.drop('name', axis=1),
        left_on="create_time", right_on="created_time",
        left_by="operator", right_by="ID", direction='backward')
    merge_df = merge_df.rename(columns={'type_x': 'type', 'type_y': 'punch_type'})
    merge_df['punch_type'] = merge_df['punch_type'].astype('str')
    merge_df['merge_type'] = merge_df['punch_type'].str.replace('_4floor', '')
    merge_df['valid_time'] = (merge_df['create_time'] >= merge_df['created_time']) & (merge_df['create_time'] <= merge_df['end_time'])
    merge_df['valid_type'] = (merge_df['type'].values == merge_df['merge_type'].values) & merge_df['valid_time']
    merge_df['Check Result'] = np.where(merge_df['valid_time'].values,
                                        np.where(merge_df['valid_type'].values, 'Correct', 'Wrong Station'),
                                        'No data')
    merge_df['created_time'] = np.where(merge_df['Check Result'].values == 'No data',
                                        np.datetime64('NaT'),
                                        merge_df['created_time'].values)
    merge_df['end_time'] = np.where(merge_df['Check Result'].values == 'No data',
                                    np.datetime64('NaT'),
                                    merge_df['end_time'].values)
    merge_df['print_label'] = np.where(merge_df['Check Result'].values == 'Wrong Station',
                                       merge_df['revised station'].values,
                                       np.nan)
    return merge_df


# In[14]:


# Checkpoint 5-2: 將merge_df依各種工作種類合併(位於calculate_score.py)
def get_valid_csv(merge_df, cat_name_checked):
    '''
    將5-1 merge_df的結果依不同cat_type分別儲存成csv檔
    input:
    1. merge_df
    2. cat_name_checked: 目前不分樓層
    '''
    valid_whole_df = merge_df[['name', 'operator', 'type', 'create_time', 'pcs', 'box', 'orders', 'total_pcs',
                               'Check Result', 'created_time', 'end_time', 'print_label']]
    for cat in cat_name_checked:
        cat_df = valid_whole_df[valid_whole_df['type'] == cat]
        cat_df.to_csv('Output/incentive_checked/{}.csv'.format(cat), encoding="utf_8_sig")


# In[15]:


# Checkpoint 6: 計算productivity_agent
def get_prod_agent_score(cat_name, productivity_varable, whole_df, punch_df, agent_output_path):
    '''
    計算Agent的Productivity Score
    input:
    1. cat_name: 工作type的list
    2. productivity_varable: 每種工作type的IPH績效
    3. whole_df: 結合IB、OB、INV的資料
    4. punch_df: 整理後打卡記錄表
    output: 計算績效的DataFrame
    '''
    # 1. punch_ids人員資料
    punch_ids = punch_df[['ID', 'name', 'role', 'class', 'group']].drop_duplicates().set_index('ID').sort_index()
    merge_df = get_merge_df(whole_df, punch_df)
    merge_df['type'] = np.where(merge_df['punch_type'].str.contains('_4floor'),
                                merge_df['punch_type'], merge_df['type'])
    punch_df['DL'] = punch_df['type'].notnull()  # 有沒有對應的cat_type

    # 2. DL_count工作時數及有在cat_type的時間比例
    DL_count = pd.crosstab(punch_df['ID'], punch_df['DL'], values=punch_df['hour'], aggfunc=np.sum)
    DL_count.fillna(0, inplace=True)
    DL_count.columns = ['not_DL', 'DL']
    DL_count['total'] = DL_count['DL'].values + DL_count['not_DL'].values
    DL_count['DL%'] = DL_count['DL'].values / DL_count['total'].values
    DL_count = DL_count[['DL', 'not_DL',  # 有cat_type的工作時數、沒有cat_type的工作時數
                         'total', 'DL%']]  # 總時數、有cat_type的工作時數的比例

    # 3. pcs_count完成數量資訊
    pcs_count = pd.crosstab(merge_df['operator'], merge_df['type'], values=merge_df['total_pcs'], aggfunc=np.sum).add_prefix('PCS_')
    for cat in cat_name:
        if 'PCS_{}'.format(cat) not in pcs_count.columns:
            print('whole_df 無 {} 資料'.format(cat))
            pcs_count['PCS_{}'.format(cat)] = 0
    pcs_count = pcs_count[['PCS_{}'.format(cat) for cat in cat_name]]

    # 4. hour_count工作時數資訊
    hour_count = pd.crosstab(punch_df['ID'], punch_df['type'], values=punch_df['hour'], aggfunc=np.sum).add_prefix('Hour_')
    for cat in cat_name:
        if 'Hour_{}'.format(cat) not in hour_count.columns:
            hour_count['Hour_{}'.format(cat)] = 0
    
    hour_count = hour_count[['Hour_{}'.format(cat) for cat in cat_name]]
    
    # productivity_table合併punch_ids, DL_count, pcs_count, hour_count
    productivity_table = punch_ids.merge(DL_count, left_index=True, right_index=True, how='left')                                  .merge(pcs_count, left_index=True, right_index=True, how='left')                                  .merge(hour_count, left_index=True, right_index=True, how='left')
    # 計算IPH分數: Hour = 0就是0，不然就是PCS/Hour
    for cat in cat_name:
        productivity_table['IPH_{}'.format(cat)] = np.where(productivity_table['Hour_{}'.format(cat)] == 0, 0,
                                                            productivity_table['PCS_{}'.format(cat)] / productivity_table['Hour_{}'.format(cat)])
    for cat in cat_name:
        productivity_table['HR%_{}'.format(cat)] = productivity_table['Hour_{}'.format(cat)] / productivity_table['DL']

    # IPH與目標的差距
    for cat in cat_name:
        productivity_table[cat] = productivity_table['IPH_{}'.format(cat)] / productivity_varable[cat]

    # 計算Productivity Score
    scores = pd.DataFrame()
    for cat in cat_name:
        scores[cat] = productivity_table[cat].values * productivity_table['HR%_{}'.format(cat)].values
    scores['Productivity Score'] = scores.sum(axis=1)
    scores.index = productivity_table.index

    # 把Productivity Score合併至productivity_table
    productivity_table = productivity_table.merge(scores[['Productivity Score']], left_index=True, right_index=True)
    productivity_table.fillna(0, inplace=True)
    productivity_table.reset_index(inplace=True)
    productivity_table.to_excel(agent_output_path, index=False)
    return productivity_table


# In[16]:


# Checkpoint 7: 計算productivity_TL
def get_prod_TL_score(productivity_varable, team_prod_dict, whole_df, punch_df, tl_output_path):
    '''
    計算Team Lead的Productivity Score
    Team Lead: 只要打卡紀錄function_name出現過MGMT即視為Team Lead，但只計算每次打卡期間超過30分鐘的打卡
    input:
    1. productivity_varable: s每種工作type的IPH績效
    3. whole_df: 結合IB、OB、INV的資料
    4. punch_df: 整理後打卡記錄表
    output: 計算績效的DataFrame
    '''
    merge_df = get_merge_df(whole_df, punch_df)
    merge_df['type'] = np.where(merge_df['punch_type'].str.contains('_4floor'), merge_df['punch_type'], merge_df['type'])
    iph = merge_df.groupby(['ID', 'type', 'created_time', 'end_time', 'hour'])['total_pcs'].agg(np.sum).reset_index()                  .rename(columns={'sum': 'total_pcs'})
    iph['function'] = iph['type'].map(team_prod_dict)

    def prod_ratio_calculate(iph, function, start, end):
        iph_ckeck = iph[(iph['function'].values == function) &
                        (iph['created_time'].values <= end) &  # 在該段時間內該cat_type的站點打卡
                        (iph['end_time'].values >= start)]\
                        .groupby(['ID', 'type'])[['hour', 'total_pcs']].agg(np.sum).reset_index()
        iph_ckeck['hour'] = np.where(iph_ckeck['hour'].values == 0, 0.008333, iph_ckeck['hour'].values)  # 0分鐘的資料在此算30秒(0.008333小時)
        iph_ckeck['iph'] = iph_ckeck['total_pcs'].values / iph_ckeck['hour'].values
        iph_ckeck['meet_goal'] = np.where(iph_ckeck['iph'].values >= iph_ckeck['type'].map(productivity_varable), 1, 0)
        return pd.Series([np.sum(iph_ckeck['meet_goal']),
                          iph_ckeck.shape[0]])
    
    # 1. team_df：以每次打卡記錄計算
    team_df = punch_df[(punch_df['function_name'] == 'MGMT') & (punch_df['min'] >= 30)]  # 只要function_name有出現過MGMT就算Team Lead，只計算控場超過30分鐘的資料
    team_df[['arrive_thres', 'count']] = team_df.apply(lambda row: prod_ratio_calculate(iph, row['function'], row['created_time'], row['end_time']), axis=1)
    team_df['prod_hour_ratio'] = np.where(team_df['count'] == 0, 0, team_df['arrive_thres'].values / team_df['count'].values)

    # 2. team_df_day：以每天打卡記錄計算
    team_df_day = team_df.groupby(['ID', 'name', 'date', 'function'])[['hour', 'arrive_thres', 'count']].agg(np.sum).reset_index()
    team_df_day['prod_day_ratio'] = np.where(team_df_day['count'] == 0, 0, team_df_day['arrive_thres'].values / team_df_day['count'].values)

    # 3. productivity_tl：該月每個team lead負責控場天數及平均達標率（若一天有兩種控場，算兩天）
    productivity_tl = team_df_day.groupby(['ID', 'name'])['prod_day_ratio'].agg(['count', np.mean]).reset_index()
    productivity_tl = productivity_tl.rename(columns={'count': 'days_on_duty', 'mean': 'TL_produtivity_score'})

    # 4. productivity_team_function：該月每個team lead每天控場達標率
    productivity_team_function = pd.crosstab(
        [team_df_day['ID'], team_df_day['name'], team_df_day['function']],
        team_df_day['date'],
        values=team_df_day['prod_day_ratio'], aggfunc='mean')
    productivity_team_function['date_on_duty'] = productivity_team_function.count(axis=1)
    productivity_team_function.reset_index(inplace=True)

    with pd.ExcelWriter(tl_output_path) as writer:
        team_df.to_excel(writer, sheet_name='team_df', index=False, encoding="utf_8_sig")
        team_df_day.to_excel(writer, sheet_name='team_df_day', index=False, encoding="utf_8_sig")
        productivity_tl.to_excel(writer, sheet_name='productivity_tl', index=False, encoding="utf_8_sig")
        productivity_team_function.to_excel(writer, sheet_name='productivity_team_function', index=False, encoding="utf_8_sig")


# In[17]:


def get_everyday_print_data(day, print_gsheet):
    day_obj = datetime.datetime.strptime(day, '%Y-%m-%d')
    SAMPLE_RANGE_NAME = "{}/{}".format(day_obj.month, day_obj.day) # 抓幾月幾號的表
    cols = ['是否印標', '印標人員', 'Tracking ID', '尾碼', 'SKU ID', 'DATE']
    try:
        print_df = pd.DataFrame(print_gsheet.worksheet(SAMPLE_RANGE_NAME).get_all_values())
        print_df = print_df.rename(columns={
            print_df.columns[0]: cols[0],
            print_df.columns[1]: cols[1],
            print_df.columns[2]: cols[2],
            print_df.columns[3]: cols[3], 
            print_df.columns[4]: cols[4], 
            print_df.columns[17]: cols[5], 
        })
        print_df.filter(items=cols)
        print_df = print_df[(print_df['是否印標'] == 'V') & (print_df['SKU ID'] != '不用印')]
        print_df.drop_duplicates(subset=['Tracking ID'], keep='first', inplace=True)
        print('get {} data'.format(day))
    except:  # 該天無印標資料
        print_df = pd.DataFrame(columns=cols)
        print('g-doc no data: {}'.format(day))
    
    return print_df


# In[18]:


def get_everyday_tag_data(day, tag_gsheet):
    SAMPLE_RANGE_NAME = day.replace("-", "")  # 抓幾月幾號的表，例如2021-06-01就抓20210601
    columns = ['版標流水號', '貼標開始', '貼標結束', '是否結束', '花費時間', '貼標人數(人)', '貼標ID']
    try:
        tag_df = pd.DataFrame(tag_gsheet.worksheet(SAMPLE_RANGE_NAME).get_all_records())
        tag_df.columns = columns
        tag_df.dropna(axis=0, inplace=True)
        tag_df['operator'] = tag_df['貼標ID'].astype("str").str.lower()
        tag_df['貼標人數(人)'] = tag_df['貼標人數(人)'].astype('int')
        print('get {} data'.format(day))
    except:  # 該天無印標資料
        print('g-doc no data: {}'.format(SAMPLE_RANGE_NAME))
        tag_df = pd.DataFrame(columns=columns)

    return tag_df


# In[19]:


# Checkpoint 8: 將merge_df進行validation，產出 valid_whole_df
def get_valid_whole_df(merge_df):
    '''
    將5-1的 merge_df 按照以下規則進行篩選：
    1. 打卡時間位於 punch starting time and punch ending time
    2. RT_putaway 和 Putaway 為 1. 之例外 
    '''

    merge_df["keep"] = merge_df["valid_type"]

    for index, row in merge_df.iterrows():
        if row["type"] == "Putaway" and row["merge_type"] == "RT_putaway":
            merge_df.loc[index, "keep"] = True
        elif row["type"] == "RT_putaway" and row["merge_type"] == "Putaway":
            merge_df.loc[index, "keep"] = True

    valid_whole_df = merge_df.copy()
    valid_whole_df = valid_whole_df[valid_whole_df["keep"]]
    valid_whole_df = valid_whole_df[['name', 'operator', 'type', 'create_time', 'pcs', 'box', 'orders', 'total_pcs']]
    valid_whole_df.to_csv("tmp_output/valid_whole_df_{}.csv".format(month_fullname), encoding="utf_8_sig", index=False)

    return valid_whole_df


# In[20]:


def output_foler(month_fullname):

    if not os.path.exists("Output/"):
        os.makedirs("Output/")
    if not os.path.exists("tmp_output/"):
        os.makedirs("tmp_output/")
    if not os.path.exists("Output/incentive_checked"):
        os.makedirs("Output/incentive_checked")


# In[21]:


if __name__ == '__main__':

    # Time
    month = "2022-04"
    start_day, end_day = "2022-04-01", "2022-04-30"
    month_first_day = datetime.datetime.strptime(month, "%Y-%m")
    month_num = str(month_first_day.month)  # 得到str月份
    month_shortname = month_first_day.strftime("%b")  # e.g. Jul, Jun
    month_fullname = month_first_day.strftime("%B")  # e.g. July, June
    
    # Input Files
    punch_file_name = 'Input/punch_for-attendance_{}.xlsx'.format(month_fullname)
    revise_station_name = 'Input/revise_station.xlsx'
    inb_pics_file_path = 'Input/IB_production_{}.xlsx'.format(month_fullname)
    inb_pics_file_path_new = 'Input/IB_production_{}_new.xlsx'.format(month_fullname)  # IB_production增加印標、收發、貼標後會儲存在此，並做為之後計算的input
    ob_pics_file_path = 'Input/OB_production_{}.xlsx'.format(month_fullname)
    inv_pics_file_path = 'Input/INV_production_{}.xlsx'.format(month_fullname)
    wms_label = 'Input/WMS_label.csv'
    
    # Output Files
    output_foler(month_fullname)
    tl_output_path = "Output/productivity_TL_{}.xlsx".format(month_fullname)
    agent_output_path = "Output/productivity_agent_{}.xlsx".format(month_fullname)
    tl_valid_output_path = "Output/productivity_TL_{}_valid.xlsx".format(month_fullname)
    agent_valid_output_path = "Output/productivity_agent_{}_valid.xlsx".format(month_fullname)
    
    
    time0 = time.time()
    punch_df = read_punch_file(punch_file_name, revise_station_name, type_dic)
    punch_df.to_csv('tmp_output/punch_df_{}.csv'.format(month_fullname), index=False, encoding="utf_8_sig")
    punch_df.dropna(subset=['created_time', 'end_time'], axis=0, inplace=True)
    time1 = time.time()
    print('Checkpoint 1 read_punch_file SUCCEED      Spend {:.2f} seconds'.format(time1 - time0))
    
    name_id_dic, id_name_dic, pda_name_dic, pda_id_dic = read_human_data()
    time2 = time.time()
    print('Checkpoint 2 read_human_datas SUCCEED     Spend {:.2f} seconds'.format(time2 - time1))

    add_data_in_inb(time2)
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
    whole_df.to_csv('tmp_output/whole_df_{}.csv'.format(month_fullname), index=False, encoding="utf_8_sig")
    time4_4 = time.time()
    print('Checkpoint 4-4 whole_df SUCCEED           Spend {:.2f} seconds'.format(time4_4 - time4_3))

    time4 = time.time()
    print('Checkpoint 4 whole_df SUCCEED             Spend {:.2f} seconds'.format(time4 - time3))

    merge_df = get_merge_df(whole_df, punch_df)
#     merge_df.to_csv("tmp_output/merge_df_{}.csv".format(month_fullname), index=False, encoding="utf_8_sig")
    time5_1 = time.time()
    print('Checkpoint 5-1 get_merge_df SUCCEED       Spend {:.2f} seconds'.format(time5_1 - time4))

    get_valid_csv(merge_df, cat_name_checked)
    time5_2 = time.time()
    print('Checkpoint 5-2 get_valid_csv SUCCEED      Spend {:.2f} seconds'.format(time5_2 - time5_1))
    time5 = time.time()
    print('Checkpoint 5 SUCCEED   Spend {:.2f} seconds'.format(time5 - time4))


    get_prod_agent_score(cat_name, productivity_varable, whole_df, punch_df, agent_output_path)
    time6 = time.time()
    print('Checkpoint 6 productivity_agent SUCCEED   Spend {:.2f} seconds'.format(time6 - time5))

    get_prod_TL_score(productivity_varable, team_prod_dict, whole_df, punch_df, tl_output_path)
    time7 = time.time()
    print('Checkpoint 7 productivity_TL SUCCEED      Spend {:.2f} seconds'.format(time7 - time6))

    valid_whole_df = get_valid_whole_df(merge_df)

    valid_whole_df.dropna(axis=0, inplace=True)
    time8 = time.time()
    print('Checkpoint 8 get_valid_whole_df SUCCEED      Spend {:.2f} seconds'.format(time8 - time7))

    get_prod_agent_score(cat_name, productivity_varable, valid_whole_df, punch_df, agent_valid_output_path)
    time9 = time.time()
    print('Checkpoint 9 productivity_valid_agent SUCCEED     Spend {:.2f} seconds'.format(time9 - time8))

    get_prod_TL_score(productivity_varable, team_prod_dict, valid_whole_df, punch_df, tl_valid_output_path)
    time10 = time.time()
    print('Checkpoint 10 productivity_valid_TL SUCCEED        Spend {:.2f} seconds'.format(time10 - time9))

    print('計算完成 共花費{:.2f}秒'.format(time10 - time0))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




