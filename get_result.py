# -*- coding: utf-8 -*-
from pymongo import MongoClient

conn = MongoClient("localhost",27017)
db = conn.job

transitlist = db.transit.find({"routes.duration":{"$lt":2000}})

companylist = list(db.companylist.find({"CompanyId":{"$in":[x["CompanyId"] for x in transitlist]}}))
print(len(companylist))

joblist = list(db.joblist.find({"CompanyId":{"$in":[x["CompanyId"] for x in companylist]}}))

with open("c:/joblist.txt","w") as f:
    f.write(str(joblist))
