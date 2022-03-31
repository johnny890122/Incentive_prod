from calendar import monthrange
from googleapiclient.discovery import build
import pandas as pd
import os
import pickle as pkl
import datetime


def get_google_sheet(SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME):
    '''
    將指定輸入的工作表欄位輸出
    '''
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pkl.load(token)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=SAMPLE_RANGE_NAME).execute()
    values = result.get('values', [])
    return values


def get_tag_data(month_first_day, month_path, *trans):
    '''
    抓取一個月tag的資料，並存在指定的path中
    input:
    1. month_first_day:抓取哪個月的資料，該月的第一天
    2. month_path:儲存路徑
    3. *trans:gdoc路徑
    output:
    tag_df:csv檔
    '''
    def get_everyday_tag_data(time, SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME):
        SAMPLE_RANGE_NAME = time.strftime("%Y%m%d") + SAMPLE_RANGE_NAME  # 抓幾月幾號的表，例如2021-06-01就抓20210601
        try:
            tag_data = pd.DataFrame(get_google_sheet(SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME))
            tag_data.columns = ['版標流水號', '貼標開始', '貼標結束', '是否結束', '花費時間', '貼標人數(人)', '貼標ID']
            tag_data.dropna(axis=0, inplace=True)
            return tag_data
        except:  # 該天無印標資料
            print('g-doc no data: {}'.format(SAMPLE_RANGE_NAME))
            return pd.DataFrame()
    no = 0
    day_range = range(monthrange(month_first_day.year, month_first_day.month)[1])
    for t in day_range:  # 需要每一天都有一張表
        time = month_first_day + datetime.timedelta(days=t)  # 要抓取的日期
        tag_df_ = get_everyday_tag_data(time, *trans)  # 抓取Gdoc
        if tag_df_.shape[0] > 0:
            if no == 0:
                tag_df = tag_df_
                no += 1
            else:
                tag_df = tag_df.append(tag_df_)
    tag_df['operator'] = tag_df['貼標ID'].str.lower()
    tag_df['貼標人數(人)'] = tag_df['貼標人數(人)'].astype('int')
    tag_df.to_csv(month_path, encoding="utf_8_sig")
    return tag_df


def get_print_data(month_first_day, month_path, *trans):
    '''
    抓取一個月print的資料，並存在指定的path中
    input:
    1. month_first_day:抓取哪個月的資料，該月的第一天
    2. month_path:儲存路徑
    3. *trans:gdoc路徑
    output:
    print_df:csv檔
    '''
    def get_everyday_print_data(time, SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME):
        SAMPLE_RANGE_NAME = str(time.month) + '/' + str(time.day) + SAMPLE_RANGE_NAME  # 抓幾月幾號的表，例如2021-06-01就抓6/1
        try:
            values = get_google_sheet(SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        except:  # 該天無印標資料
            print('g-doc no data: {}'.format(SAMPLE_RANGE_NAME))
            values = []
        print_ = pd.DataFrame(values)
        return print_

    no = 0
    day_range = range(monthrange(month_first_day.year, month_first_day.month)[1])
    for t in day_range:  # 需要每一天都有一張表
        time = month_first_day + datetime.timedelta(days=t)  # 要抓取的日期
        print_df_ = get_everyday_print_data(time, *trans)  # 抓取Gdoc
        if print_df_.shape[0] > 0:
            if no == 0:
                print_df = print_df_[[0, 1, 2, 3, 4, 17]]
                no += 1
            else:
                print_df = print_df.append(print_df_[[0, 1, 2, 3, 4, 17]])
    print_df.columns = ['是否印標', '印標人員', 'Tracking ID', '尾碼', 'SKU ID', 'DATE']
    print_df = print_df[(print_df['是否印標'] == 'V') & (print_df['SKU ID'] != '不用印')]
    print_df.drop_duplicates(subset=['Tracking ID'], keep='first', inplace=True)
    print_df.to_excel(month_path)
    return print_df
