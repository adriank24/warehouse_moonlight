#!/usr/bin/env python
# coding: utf-8

# Dependency Library

# In[1]:


import pymongo
import datetime
import schedule
import time

warehouse = 'mongodb+srv://admin:WeaYWh2g5ngfNK9@cluster0.khbod.mongodb.net/<moonlight>?retryWrites=true&w=majority'
backend='mongodb+srv://data_warehouse:xLbTHZqmnHjxiXsS@clusterbelajar.mp6dn.mongodb.net/tni_al?retryWrites=true&w=majority'
client_dw = pymongo.MongoClient(warehouse)
client_be = pymongo.MongoClient(backend)
#DB yang dipake
dwdb = client_dw["moonlight"]
bedb = client_be["tni_al"]


# Get new data from vessel and insert to AIS

# In[ ]:


def sinkron_ais():
    ts=datetime.datetime.now()
    #Collection yang dipake
    vessel = bedb["vessels"]
    log_col = dwdb["vessel_log"]
    ais = dwdb["ais"]
    #data jumlah data vessel seluruh
    vessel_data=vessel.find()
    vessel_num=vessel_data.count()

    #data log paling anyar
    log = log_col.find().sort('date', -1)
    data_log=log[0]['cur_vessel']

    if (vessel_num>data_log):
        print('New vessel added')
        for x in range(data_log,vessel_num):
            data=vessel_data[x]
            data["callsign"]="kijang 1"
            data["lat"]=3.94215
            data["lon"]=108.20755
            data["speed"]=0
            data["heading"]=0
            data["course"]=0
            data["status"]=0
            data["timestamp"]=ts
            data["destination"]="null"
            data["eta"]="2016-04-18T12:21:00.000+00:00"
            data["data_source"]="Kijang2"
            data["is_assignment"]=False
            data['is_mission']=False
            
            log_entry = { "date": ts, "cur_vessel": vessel_num}
            log_col.insert_one(log_entry)
            print('log updated')
            try:
                agregasi_category1(data)
                ais.insert_one(data)
                print('Data added to AIS')
            except pymongo.errors.DuplicateKeyError:
                # skip document because it already exists in new collection
                print('Vessel already added to AIS')
                continue
                
    elif (vessel_num<data_log):
        log_entry = { "date": ts, "cur_vessel": vessel_num}
        print(log_entry)
        log_col.insert_one(log_entry)
    else:
        print('Data sudah sinkron') 


# Agregasi data category kapal pada data AIS

# Proto

# In[ ]:


def agregasi_category1(data_ais):
    ts=datetime.datetime.now()
    #Collection yang dipake
    category = bedb["vesselcategories"]
    ais=dwdb["ais"]
    
    category_id=data_ais["ship_category"]
    data_category=category.find_one({ "_id":category_id })
    if data_category!=None:
        data_ais['ship_category']=data_category['name']
        ais.insert_one(data_ais)
        print('data agregated')
    else:
        ais.insert_one(x)
        print("data already correct")    


# 

# Agregasi status assignment

# In[2]:


def agregasi_tugas():
    #Collection yang dipake
    tugas=bedb["assignments"]
#     misi=bedb["specialmissions"]
    vessel=bedb["vessels"]
    ais=dwdb["ais"]
    
    data_tugas=tugas.find()
    for x in data_tugas:
        if x["draft"]== True:
            continue
        else:
            if "vessel" in x:
                data_vessel=vessel.find_one({"_id":x["vessel"]})
            else:
                continue
            data_vessel=vessel.find_one({"_id":x["vessel"]})
            if x["completed"]== True:
                myquery = { "mmsi": data_vessel["mmsi"] }
                newvalues = { "$set": { "is_assignment": False } }
                ais.update_one(myquery,newvalues)
            else:
                myquery = { "mmsi": data_vessel["mmsi"] }
                newvalues = { "$set": { "is_assignment": True, "lat":x["lat"], "lon":x["lon"] } }
                ais.update_one(myquery,newvalues)
                
    print("assignment sinkron")          


# 

# Agregasi status mission

# In[4]:


def agregasi_mission():
    #Collection yang dipake
    misi=bedb["specialmissions"]
    vessel=bedb["vessels"]
    ais=dwdb["ais"]
    
    data_misi=misi.find()
    for x in data_misi:
        if "vessel" in x:
            data_vessel=vessel.find_one({"_id":x["vessel"]})
        else:
            continue
            
        if x["completed"]== True:
            myquery = { "mmsi": data_vessel["mmsi"] }
            newvalues = { "$set": { "is_assignment": False, "is_mission": False } }
            ais.update_one(myquery,newvalues)
        else:
            myquery = { "mmsi": data_vessel["mmsi"] }
            newvalues = { "$set": { "is_assignment": True, "is_assignment": True, "lat":x["lat"], "lon":x["lon"] } }
            ais.update_one(myquery,newvalues)
                
    print("mission sinkron")      


# In[5]:


schedule.every(1).minutes.do(sinkron_ais)
schedule.every(3).minutes.do(agregasi_tugas) 
schedule.every(3).minutes.do(agregasi_mission)
while 1 :
    schedule.run_pending()
    time.sleep(1)


# In[ ]:




