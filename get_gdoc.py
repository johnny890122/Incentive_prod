from calendar import monthrange
from googleapiclient.discovery import build
import pandas as pd
import os
import pickle as pkl
import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json

def get_google_sheet(SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME):
    '''
    將指定輸入的工作表欄位輸出
    '''
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("here")
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    exit()
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
        SAMPLE_RANGE_NAME = time.strftime("%Y%m%d")  # 抓幾月幾號的表，例如2021-06-01就抓20210601
        try:
            # tag_data = pd.DataFrame(get_google_sheet(SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME))
            tag_data = pd.read_excel("tmp_input/新版貼標紀錄備份.xlsx", sheet_name=SAMPLE_RANGE_NAME)
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
        SAMPLE_RANGE_NAME = str(time.month) + str(time.day) # 抓幾月幾號的表，例如2021-06-01就抓6/1
        try:
            print_ = pd.read_excel("tmp_input/2022印標.xlsx", sheet_name=SAMPLE_RANGE_NAME, skiprows=[0])
        #     # values = get_google_sheet(SCOPES, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        #     print_ = pd.read_excel("2022印標.xlsx", sheet=SAMPLE_RANGE_NAME)
            print('get {} data'.format(str(time.month) + "/" + str(time.day)))
        except:  # 該天無印標資料
            print_ = pd.DataFrame([])
            print('g-doc no data: {}'.format(str(time.month) + "/" + str(time.day)))
            # values = []
        # print_ = pd.DataFrame(values)
        # print_ = pd.read_excel("2022印標.xlsx", sheet_name=SAMPLE_RANGE_NAME)
        return print_

    no = 0
    day_range = range(monthrange(month_first_day.year, month_first_day.month)[1])
    for t in day_range:  # 需要每一天都有一張表
        time = month_first_day + datetime.timedelta(days=t)  # 要抓取的日期
        print_df_ = get_everyday_print_data(time, *trans)  # 抓取Gdoc

        if print_df_.shape[0] > 0:
            if no == 0:
                print_df = print_df_.filter(items=[print_df_.columns[0], print_df_.columns[1], print_df_.columns[2], print_df_.columns[3], print_df_.columns[4], print_df_.columns[17]])
                no += 1
            else:
                print_df = print_df.append(
                    print_df_.filter(items=[print_df_.columns[0], print_df_.columns[1], print_df_.columns[2], print_df_.columns[3], print_df_.columns[4], print_df_.columns[17]])
                    )
    print_df.columns = ['是否印標', '印標人員', 'Tracking ID', '尾碼', 'SKU ID', 'DATE']
    print_df = print_df[(print_df['是否印標'] == 'V') & (print_df['SKU ID'] != '不用印')]
    print_df.drop_duplicates(subset=['Tracking ID'], keep='first', inplace=True)
    print_df.to_excel(month_path)
    return print_df
