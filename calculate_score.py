import numpy as np
import pandas as pd


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

# Checkpoint 5-3: 將merge_df進行validation，產出 valid_whole_df
def get_valid_whole_df(merge_df):
    '''
    將5-1的 merge_df 按照以下規則進行篩選：
    1. 打卡時間位於 punch starting time and punch ending time
    2. RT_picking 和 Picking 為 1. 之例外 
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
    valid_whole_df.to_csv("valid_whole_df.csv", encoding="utf_8_sig", index=False)

    return valid_whole_df

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
    for cat in hour_count:
        if 'PCS_{}'.format(cat) not in hour_count.columns:
            hour_count['PCS_{}'.format(cat)] = 0
    hour_count = hour_count[['Hour_{}'.format(cat) for cat in cat_name]]

    # productivity_table合併punch_ids, DL_count, pcs_count, hour_count
    productivity_table = punch_ids.merge(DL_count, left_index=True, right_index=True, how='left')\
                                  .merge(pcs_count, left_index=True, right_index=True, how='left')\
                                  .merge(hour_count, left_index=True, right_index=True, how='left')

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
    iph = merge_df.groupby(['ID', 'type', 'created_time', 'end_time', 'hour'])['total_pcs'].agg(np.sum).reset_index()\
                  .rename(columns={'sum': 'total_pcs'})
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
