## this is version that utilize DB store and fetch data
import json

import os
from Module.DBHelper import DBHelper

import time
import datetime

import requests



# 建立DB連線
DB_WORK = DBHelper(os.environ['DB_HOST'],os.environ['DB_USER'],os.environ['DB_PW'],os.environ['DB_NAME'],'')
DB_CURSOR = DB_WORK.cursor
DB_TABLE = 'podcast_data'
PODCAST_DATA_COLUMNS = ['id', 'programme', 'download', 'record_date']

def main(event, context):
    token            = get_tokens()
    main_mes         = "\n【Podcast週報】本週各節目下載量表現\n"
    data_db          = []
    for web in event['key1']:
        print(web)
        # channel_mes會依照channel_name決定接下來要跑get_download_data / get_download_data_soundon
        # 並且用download_message去接return
        # get_rank_data每個channel都在同一個web
        download_message   = channel_mes(channel_name = web)
        last_week_data     = get_last_week_data(web)
        # 資料用成 tuple準備寫到 DB
        data_db.append(tuple(download_message))
        
        if len(last_week_data) != 0:
            last_week_download = last_week_data[2]
            message          = "\n{}：\n下載數{}次，\n較上週比較{}\n".format(download_message[0], download_message[1], (download_message[1] - last_week_download) / last_week_download)
            main_mes        += message
        else:
            message          = "\n{}：\n下載數{}次，\n較上週比較{}\n".format(download_message[0], download_message[1], "上週尚無資料")
            main_mes        += message
            
    # 寫入資進 DB
    write_data_to_db(data_db)
    # 送 line msg
    send_line_msg(token, main_mes)
    print(data_db)
    return None

def get_last_week_data(web):
    sql = """SELECT *  FROM `podcast_data` 
             WHERE 'record_data' = CURRENT_DATE - interval '7days'
             AND 'programme' = '{}'
    """.format(web)
    try:
        DB_CURSOR.executey(sql)
        record = [row for row in DB_CURSOR.fetchall()]
    except:
        print('get_last_week ERROR!')
        record = []
    return record

def write_data_to_db(insert_data):
    sql = """INSERT INTO `podcast_data` (`programme`, `download`, `record_date`) VALUES (%s, %s, %s)"""
    
    print(sql)
    DB_CURSOR.executemany(sql, insert_data)
    DB_WORK.connection.commit()
    
def channel_mes(channel_name):
    if channel_name == '創業新聲帶':
        # 創業在sound_on
        return get_download_data_soundon(web = channel_name)
    if channel_name == '6 in 5' or channel_name == '每日聽管理':
        return transistor_podcast(web = channel_name)
    else:
        return get_download_data(web = channel_name)


# 變成是這週的
def get_download_data(web = '數位時代'):
    today       = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d")
    last_week   = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=6),"%Y-%m-%d")
    if web == '數位時代':
        email    = "***"
        password = "***"
    elif web == '經理人':
        email    = "***"
        password = "***"
    elif web == '設計關鍵字':
        email    = "***"
        password = "***"

    payload_1  = {
    "variables" : {
                   "email": email, 
                   "password": password
    },
    "query":"mutation AuthLoginWithEmail($email: String!, $password: String!) {\n  authLoginWithEmail(email: $email, password: $password) {\n    ...AuthPayload\n    __typename\n  }\n}\n\nfragment AuthPayload on AuthPayload {\n  token\n  isTwoFa\n  show {\n    id\n    name\n    __typename\n  }\n  __typename\n}\n"
    }

    headers = {
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Headers":"Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Authorization"
    }

    s        = requests.session()
    response = s.post('https://prod-api.firstory.me/hosting/v1/graphql', json = payload_1, headers = headers)

    # 抓bearer資料
    if web != '經理人':
        headers['Authorization'] = 'Bearer ' + response.json()['data']['authLoginWithEmail']['token']
    else:
        headers['Authorization'] = 'Bearer eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJjZXJ0IjoiY2wwcTFlNG13NjgyZTZ4M3Y3aXllMHZseiIsInVzZXJJZCI6ImNrdWY4djN0dWE0aG0wODczdzVkM2l3eGsiLCJzaG93SWQiOiJja3VmYWliYzVkYzN1MDk2NmI2cmM5NjFiIiwiaWF0IjoxNjg1NTQ3NTc3fQ.tdYDkP8lOV2QYsJTxwQ1zJHdUzRVxui2TQZq77mX610pFQP-o7qsnLUA_s21dhNI8HBu12pmRVbtxPp2kBaEtg'
    headers['Cache-Control'] = 'no-cache'
    headers['pragma']        = 'no-cache'

    payload_this_week  = {
        "variables" : {
                    "from": last_week, 
                    "to": today
        },
        "query":"query GetDashboardOverviewByDate($from: DateTime, $to: DateTime) {\n  studioAnalyticShowDataByDateFind(from: $from, to: $to) {\n    starts {\n      date\n      value\n      __typename\n    }\n    uniqueStarts {\n      date\n      value\n      __typename\n    }\n    spotifyStarts {\n      date\n      value\n      __typename\n    }\n    __typename\n  }\n}\n"
    }
    response_ = s.post('https://prod-api.firstory.me/hosting/v1/graphql', json = payload_this_week, headers = headers)

    download_uni = 0
    for x in response_.json()['data']['studioAnalyticShowDataByDateFind']['uniqueStarts']:
        download_uni += x['value']
        
    download_spo = 0
    for x in response_.json()['data']['studioAnalyticShowDataByDateFind']['spotifyStarts']:
        download_spo += x['value']

    this_week    = download_uni + download_spo

    return web, this_week, today

def get_download_data_soundon(web):
    today       = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d")
    payload_1  = {
        "email": "meet@bnext.com.tw", 
        "password": "meetstartup",
        "returnSecureToken":True
    }

    headers = {
        "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "Headers":"Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,Authorization"
    }

    s        = requests.session()
    response = s.post('https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyPassword?key=AIzaSyB1gTYyA_fN-YgXNxFn2JGhYjClpOH_Ew4', json = payload_1, headers = headers)
    headers['Authorization'] = 'Bearer ' + response.json()['idToken']
    url      = "https://api.soundon.fm/v2/podcasts/112db27e-1fa9-4de3-af5a-4294b2a547f9/analytics/dashboard/keyNumbers"
    data     = requests.get(url, headers=headers).json()
    download = data['data']['lastWeekCount'] 
    return web, download, today
    
def transistor_podcast(web = '每日聽管理'):
    now                  = datetime.datetime.now()
    # 過去一個禮拜的結束
    now_str              = datetime.datetime.strftime(now, '%d-%m-%Y')
    # 過去一個禮拜的開始
    one_week_start_str   = datetime.datetime.strftime(now - datetime.timedelta(days = 6), '%d-%m-%Y')

    ## 計算這個禮拜的資料

    payload_one_week = {
        "start_date" : one_week_start_str,
        "end_date"   : now_str
    }


    headers = {
        "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "x-api-key"  : "QPsA6Xm3mnEavyr6VA8AYw",
    }



    this_week_6in5    = requests.get(r"https://api.transistor.fm/v1/analytics/6-in-5", headers = headers, data = payload_one_week)
    this_week_manager = requests.get(r"https://api.transistor.fm/v1/analytics/a2563908-2dcd-4008-891b-b561dd5f68e1", headers = headers, data = payload_one_week)
)

    # 拉出來是一串dict
    if web == '6 in 5':
        download_this_week = 0
        for data in json.loads(this_week_6in5.text)['data']['attributes']['downloads']:
            download_this_week += data['downloads']
    

    else:
        download_this_week = 0
        for data in json.loads(this_week_manager.text)['data']['attributes']['downloads']:
            download_this_week += data['downloads']

    
    # now_str 的格式無法寫進去 DB
    return web, download_this_week, now_str[-4:] + '-' + now_str[-7:-5] + '-' + now_str[0:2]
    
def get_tokens() -> list:
    tokens = [
    ]
    return tokens
    
def send_line_msg(tokens: list, message: str) -> bool:
    ret_list = []
    is_succ = True
    for token in tokens:
        ret = {
            "is_succ": True,
            "err_msg": ""
        }
        payload = {
            "message": message,
        }
        headers = {
            "Authorization": "Bearer " + token
        }
        r = requests.post(
            url = "https://notify-api.line.me/api/notify",
            data = payload,
            headers = headers
        )
        if(r.status_code != 200):
            ret["is_succ"] = False
            ret["err_msg"] = f"[Token: {token}]Sending message failed. {r.json()}"
            ret_list.append(ret)
            print(f"[Token: {token}]Sending message failed. {r.json()}")

    if len(ret_list) > 0:
        is_succ = False
        
        
    return is_succ
    
    
