import json

import time
import datetime

import requests


def main(event, context):
    token            = get_tokens()
    main_mes         = "\n【Podcast週報】本週各節目下載量表現\n"
    for web in event['key1']:
        print(web)
        # channel_mes會依照channel_name決定接下來要跑get_download_data / get_download_data_soundon
        # 並且用download_message去接return
        # get_rank_data每個channel都在同一個web
        download_message = channel_mes(channel_name = web)
        # rank_info        = get_rank_data(web = web)
        rank_info        = '尚未提供'
        message          = "\n{}：\n下載數{}次，\n較上週比較{}\n".format(download_message[0], download_message[1], download_message[2])
        main_mes        += message
    send_line_msg(token, main_mes)
    return None

def channel_mes(channel_name):
    if channel_name == '創業新聲帶':
        # 創業在sound_on
        return get_download_data_soundon(web = channel_name)
    else:
        return get_download_data(web = channel_name)

def get_download_data(web = '數位時代'):
    today       = datetime.datetime.strftime(datetime.date.today(),"%Y-%m-%d")
    last_week   = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=6),"%Y-%m-%d")
    last_week_2_end   = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=7),"%Y-%m-%d")
    last_week_2_start = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=13),"%Y-%m-%d")
    if web == '數位時代':
        email    = ""
        password = ""
    elif web == '經理人':
        email    = ""
        password = ""
    elif web == '設計關鍵字':
        email    = ""
        password = ""

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


    payload_last_week  = {
        "variables" : {
                    "from": last_week_2_start, 
                    "to": last_week_2_end
        },
        "query":"query GetDashboardOverviewByDate($from: DateTime, $to: DateTime) {\n  studioAnalyticShowDataByDateFind(from: $from, to: $to) {\n    starts {\n      date\n      value\n      __typename\n    }\n    uniqueStarts {\n      date\n      value\n      __typename\n    }\n    spotifyStarts {\n      date\n      value\n      __typename\n    }\n    __typename\n  }\n}\n"
    }
    response_ = s.post('https://prod-api.firstory.me/hosting/v1/graphql', json = payload_last_week, headers = headers)

    # print(response_.json())

    download_uni = 0
    for x in response_.json()['data']['studioAnalyticShowDataByDateFind']['uniqueStarts']:
        download_uni += x['value']
        
    download_spo = 0
    for x in response_.json()['data']['studioAnalyticShowDataByDateFind']['spotifyStarts']:
        download_spo += x['value']

    last_week    = download_uni + download_spo
    
    try:
        growth_rate_int = round(((this_week - last_week) /  last_week)*100,2)
        if growth_rate_int >= 0:
            growth_rate     = "+" + str(round(((this_week - last_week) /  last_week)*100,2)) + "%"
        else:
            growth_rate     = str(growth_rate_int) + "%"
    except:
        growth_rate = 'Divided By 0'
    return web, this_week, growth_rate

def get_download_data_soundon(web):
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
    # print(response.json())
    headers['Authorization'] = 'Bearer ' + response.json()['idToken']
    # print(headers)
    url      = "https://api.soundon.fm/v2/podcasts/112db27e-1fa9-4de3-af5a-4294b2a547f9/analytics/dashboard/keyNumbers"
    data     = requests.get(url, headers=headers).json()
    # print(data)
    download = data['data']['lastWeekCount'] 
    if round(data['data']['lastWeekDelta']*100 ,2) > 0:
        compare  = "+" + str(round(data['data']['lastWeekDelta']*100 ,2)) + "%"
    else:
        compare  = str(round(data['data']['lastWeekDelta']*100 ,2)) + "%"
    return web, download, compare
    
def get_tokens() -> list:
    tokens = [
        "iQ3bds0APgRMKUqxd6Xoaw9ZHPymVMpW2pMj8Z92bwT",
        "d16loOqFSWJ6KcTED0slsArHn2JfhQtrkmMqv9nlFzt",
        "gvWOjlU1F9kOxTdG67XmpCfypSno2NyTZRnoXTGFLpo"
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
            "message": message
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
        # err_msg = '\n'.join([d['err_msg'] for d in ret_list])
        
        
    return is_succ
