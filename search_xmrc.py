# -*- coding: utf-8 -*-
import requests
import json
import time
import random
import threading
import sys
import re
from urllib import parse
from pymongo import MongoClient
from requests.adapters import HTTPAdapter
from asq import query

MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_TABLE = '127.0.0.1', '27017', 'test_db', 'teat_tb'
requests.packages.urllib3.disable_warnings() #去掉证书警告
request_retry = HTTPAdapter(max_retries=10) #超时重试适配器

conn = MongoClient("localhost",27017)
db = conn.job
#百度地图应用ak
ak=''

#获取家的经纬度 从地址搜或从当前ip获取
addr = input("地址：")
if not addr.strip():
   r =
   requests.get(f"http://api.map.baidu.com/location/ip?ak={ak}&coor=bd09ll")
   if r.json()["status"]==0:
       lng = r.json()["point"]["x"]
       lat = r.json()["point"]["y"]
   else:
       print(r.json()["message"])
       input("...")
       sys.exit()
else:
   #region厦门 以第一条作为结果
   r =
   requests.get(f"http://api.map.baidu.com/place/v2/search?query={parse.quote(addr)}&region=%E5%8E%A6%E9%97%A8&output=json&ak={ak}")
   if r.json()["status"]==0:
       lng = r.json()["results"][0]["location"]["lng"]
       lat = r.json()["results"][0]["location"]["lat"]
   else:
       print(r.json()["message"])
       input("...")
       sys.exit()

if not lng or not lat:
   print("获取经纬度失败")
   input("...")
   sys.exit()
#或者直接填写百度地图系坐标
#lat,lng = 0,0

key = input("关键字：")

#从xmrc微信端获取
r = requests.post(url="https://www.xmrc.com.cn/Resource/Api/RecruitInfoHandler.ashx?action=QueryRecruits",
                verify=False,
                data={
                    "SearchType":"1",
                    "Keyword":key,
                    "AdvanceSearchFlag":"0",
                    "PageIndex":"1",
                    "pageSize":"300",
                    "m":"RecruitInfoHandler.ashx?action=QueryRecruits",
                    "appName":"pad.user",
                    "appVersionInt":"1",
                    "debug":"0",
                    "did":"9A1DXWON8DFYH328SOPIOH4HUE7KPEBS",
                    "timestamp":"1525329169339",
                    "verifyCode":"123",
                    "loginUserId":"0",
                    "imUserId":"",
                    "remote_user_id":"1",
                    "remote_verify_code":"123"
                    },
                headers={
                    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat QBCore/3.43.691.400 QQBrowser/9.0.2524.400",
                    "Referer": "https://www.xmrc.com.cn/pad3/talent/pages/job_list.html"
                    })
res = r.json()
res["Keyword"] = key
data = res["Data"]

result = db.serachresult
result.insert(res)

#with open("c:/data.txt","w") as f:
#    f.write(json.dumps(data))
#print(len(data))
def getDetail_xmrc(rid):
    s = requests.Session()
    s.mount('https://',request_retry)
    r = s.post(url="https://www.xmrc.com.cn/Resource/Api/RecruitInfoHandler.ashx?action=GetRecruit",
                verify=False,
                data={
                    "RecruitId":rid,
                    "TalentId":"",
                    "m":"RecruitInfoHandler.ashx?action=GetRecruit",
                    "appName":"pad.user",
                    "appVersionInt":"1",
                    "debug":"0",
                    "did":"BAM653NWTHGEISMTTOTS4LA4GV7DK3C5",
                    "timestamp":"1525834155662",
                    "verifyCode":"123",
                    "loginUserId":"0",
                    "imUserId":"",
                    "remote_user_id":"1",
                    "remote_verify_code":"123"
                    },
                headers={
                    "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat QBCore/3.43.691.400 QQBrowser/9.0.2524.400",
                    "Referer": "https://www.xmrc.com.cn/pad3/talent/pages/job_detail.html"
                    })
    data = r.json()["Data"]
    db = conn.job
    #添加唯一索引
    if len(db.joblist.index_information()) == 0:
        db.joblist.create_index('RecruitId', unique=True)
    db.joblist.save(data)


def getCompany_xmrc(cid):
    s = requests.Session()
    s.mount('https://',request_retry)
    r = s.get(url="https://www.xmrc.com.cn/Resource/Api/RecruitInfoHandler.ashx?action=GetCompany",
                     verify=False,
                     data={
                         "CompanyId":cid,
                         "m":"RecruitInfoHandler.ashx%3Faction%3DGetCompany",
                         "appName":"pad.user",
                         "appVersionInt":"1",
                         "debug":"0",
                         "did":"XAH535VSUVXOMH4U9PT7UQ8AECXJOOCT",
                         "timestamp":"1526033428564",
                         "verifyCode":"123",
                         "loginUserId":"0",
                         "imUserId":"",
                         "remote_user_id":"1",
                         "remote_verify_code":"123"
                         },
                     headers={
                        "User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 MicroMessenger/6.5.2.501 NetType/WIFI WindowsWechat QBCore/3.43.691.400 QQBrowser/9.0.2524.400",
                        "Referer": "https://www.xmrc.com.cn/pad3/talent/pages/job_detail.html"
                        })
    data = r.json()["Data"]
    db = conn.job
    #添加唯一索引
    if len(db.companylist.index_information()) == 0:
        db.companylist.create_index('CompanyId', unique=True)
    #插入重复键退出
    try:
        db.companylist.insert(data)
    except:
        return

    #获取公司经纬度
    addr = data["Address"]
    #删掉全部括号内容 确保能获取到经纬度
    addr = re.sub(r'\(.*?\)|（.*?）','',addr)
    r = requests.get(f"http://api.map.baidu.com/place/v2/search?query={parse.quote(addr)}&region=%E5%8E%A6%E9%97%A8&output=json&ak={ak}")
    if r.status_code != 200:
        return
    if "results" in r.json() and r.json()["results"]:
        clng = r.json()["results"][0]["location"]["lng"]
        clat = r.json()["results"][0]["location"]["lat"]
    elif data["Map"] and "Lng" in list(data["Map"].keys()):
        clng = data["Map"]["Lng"]
        clat = data["Map"]["Lat"]
        if not clng or not clat:
            return
    else:
        return

    #存入经纬度
    db.companylist.update({"CompanyId":data["CompanyId"]},{"$set":{"longitude":clng,"latitude":clat}})

    #获取行程规划
    r = requests.get(f"http://api.map.baidu.com/direction/v2/transit?origin={lat},{lng}&destination={clat},{clng}&ak={ak}")
    if r.json()["status"] == 0:
        transit = r.json()["result"]
        transit["source"] = f"{lat},{lng}丨{clat},{clng}"
        transit["CompanyId"] = data["CompanyId"]
        if db.transit.count({"$and":[{"source":transit["source"]},{"CompanyId":data["CompanyId"]}]}) == 0:
            try:
                db.transit.insert(transit)
            except:
                print("company重复")



threads = [] #线程池

#按照公司名分组后根据职位名去重
for groupdata in query(data).order_by_descending(lambda x:x["RecruitId"]).group_by(lambda x:x["CompanyName"]):
    for x in query(groupdata).distinct(lambda x:x["JobName"]):
        threads.append(threading.Thread(target=getDetail_xmrc,args=(x.get("RecruitId"),)))
        threads.append(threading.Thread(target=getCompany_xmrc,args=(x.get("CompanyId"),)))
        #getDetail_xmrc(x.get("RecruitId"))
        #time.sleep(random.random()*10)
        #getCompany_xmrc(x.get("CompanyId"))
        #time.sleep(random.random()*10)
for t in threads:
    t.start()
    t.join()
    time.sleep(random.random())

conn.close()