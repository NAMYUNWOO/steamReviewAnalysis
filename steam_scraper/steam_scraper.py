import requests, grequests,re,sqlite3,datetime,sys,getopt,pymongo,time
from collections import deque

class SteamData:
    R_URL = "http://store.steampowered.com/appreviews/%d?json=1&filter=recent&start_offset=%d"
    GL_URL = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/?key=STEAMKEY&format=json"
    G_URL = "http://store.steampowered.com/api/appdetails?appids=%d"
    U_URL = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=%s&steamids=%s"
    UA_URL = "http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid=%d&key=%s&steamid=%s"
    US_URL = "http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid=%d&key=%s&steamid=%s"
    UO_URL = " http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=%s&steamid=%s&format=json"
    UB_URL = "http://api.steampowered.com/ISteamUser/GetPlayerBans/v1/?key=%s&steamids=%s"
    APIKEY= "yourAppkey"
    authors = set()
    authorsList = []
    currentAppId = -1
    
    
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.steam
    
    def newAppList(self):
        appList = requests.get(self.GL_URL).json()["applist"]["apps"]
        return appList
    
    def getAppids(self):
        return self.db.appList.distinct("appid")
    
    def updateAppList(self):
        newApplist = self.newAppList()
        newAppset = set([i["appid"] for i in newApplist])
        curAppset = set(self.getAppids())
        addtionalAppids = list(newAppset - curAppset)
        appid_name = {i["appid"]:i["name"] for i in newApplist}
        additionalDoc = [{"appid":i,"name":appid_name[i]} for i in addtionalAppids]
        if len(additionalDoc) == 0:
            return 
        self.db.appList.insert_many(additionalDoc)
        print("add AppList below...")
        for i in additionalDoc:
            print(i)
        
    def __getGameDetailMapper(self,x):
        try:
            return x.json()[list(x.json().keys())[0]]["data"]
        except:
            return 
    def getAppDetails(self):
        curAppidsSet = set(self.getAppids())
        updatedDetailsSet = set(self.db.appDetail.distinct('steam_appid'))
        ids2Add = curAppidsSet - updatedDetailsSet
        rs = (grequests.get(self.G_URL%(i),timeout=2) for i in list(ids2Add))
        myresponse = grequests.map(rs)
        appDetails_ = list(map(lambda x: self.__getGameDetailMapper(x),myresponse))
        appDetails = [i for i in appDetails_ if i != None]
        return appDetails
     
    
    def updateAppDetail(self):
        appDetails = self.getAppDetails()
        self.db.appDetail.insert_many(appDetails)
    
    def updateAllAppReviews(self):
        appids = self.getAppids()
        for i in appids:
            self.updateAppReviews(i)
            print("appid %d, insert job is complete"%i)
        return 
    
    
    def reqReviewList(self,appid,rng):
        rs = (grequests.get(self.R_URL%(appid,i)) for i in rng)
        rs_ = grequests.map(rs) 
        res = [j for i in rs_ if i != None for j in i.json()['reviews']]
        for res_i in res:
            res_i.update({"appid":appid})
            self.authors.add(res_i['author']['steamid'])
        return res
    
    def getAllReviews(self,appid):
        return self.db.appReview.find({"appid":appid})
    
    def updateAppReviews(self,appid):
        self.currentAppId = appid
        failCnt = 0
        insufficientCnt = 0
        if self.db.appReview.count({"appid":appid}) == 0:
            print("inserting reviews first time")
            n = 0
            while failCnt < 10:
                rng = range(n,(n+1)+100,20)
                res = self.reqReviewList(appid,rng)
                if len(res) == 0:
                    failCnt += 1
                    continue
                elif len(res) < 120:
                    print("insufficient data")
                    insufficientCnt += 1
                    if insufficientCnt < 10:
                        continue
                    
                n = rng[-1]
                failCnt = 0
                insufficientCnt = 0
                self.db.appReview.insert_many(res)
                
        else:
            print("insert additional reviews")
            lastUpdatedDt = self.db.appReview.find({"appid":appid}).sort('timestamp_created',-1)[0]['timestamp_created']
            n = 0
            isEnd = False
            inTheEnd = False
            while not isEnd:
                rng = range(n,(n+1)+100,20)
                res = self.reqReviewList(appid,rng)
                if len(res) == 0:
                    failCnt += 1
                    continue
                elif len(res) < 120:
                    print("insufficient data")
                    insufficientCnt += 1
                    if insufficientCnt < 10:
                        continue
                insufficientCnt = 0
                failCnt = 0
                n = rng[-1]
                m = len(res) -1
                while True:
                    if m < 0 :
                        isEnd = True
                        break
                    if res[m]['timestamp_created'] > lastUpdatedDt:
                        print(res[m]['timestamp_created'],lastUpdatedDt)
                        self.db.appReview.insert_many(res[:m+1])
                        if inTheEnd:
                            isEnd =True
                        break
                    else:
                        m -= 1
                        inTheEnd = True
                
        print("insert complete total review of AppID: %d is %d"%(appid,self.db.appReview.count({"appid":appid})))
        self.updateUserInfos()
        self.authors.clear()
        return
    
    
    def getUserSummary(self,userlist):
        rs = (grequests.get(self.U_URL%(self.APIKEY,steamid),timeout=10) for steamid in userlist)
        res_ = grequests.map(rs)
        docs = []
        for i,steamid_i in zip(res_,userlist):
            try:
                doc = i.json()["response"]['players'][0]
            except:
                continue
            docs.append(doc)
        return docs
        
    
    def getUserOwns(self,userlist):
        rs = (grequests.get(self.UO_URL%(self.APIKEY,steamid),timeout=10) for steamid in userlist)
        res_ = grequests.map(rs)
        docs = []
        for i,steamid_i in zip(res_,userlist):
            try:
                doc = i.json()["response"]
            except:
                continue
            doc.update({"steamid":steamid_i})
            docs.append(doc)
        return docs

    
    def getUserAchieve(self,userlist):
        rs = (grequests.get(self.UA_URL%(self.currentAppId,self.APIKEY,steamid),timeout=10) for steamid in userlist)
        res_ = grequests.map(rs)
        docs = []
        for i,steamid_i in zip(res_,userlist):
            try:
                doc = i.json()["playerstats"]
            except:
                continue
            doc.update({"appid":self.currentAppId})
            docs.append(doc)
        return docs     

    
    def getUserBan(self,userlist):
        rs = (grequests.get(self.UB_URL%(self.APIKEY,steamid),timeout=10) for steamid in userlist)
        res_ = grequests.map(rs)
        docs = []
        for i,steamid_i in zip(res_,userlist):
            try:
                doc = i.json()["players"][0]
            except:
                continue
            doc.update({"appid":self.currentAppId})
            docs.append(doc)
        return docs     
    
    def chunks(self,l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]
    
    def replaceUserCollection(self,dataArr,func,updateKeys):
        keysList = list(updateKeys.keys())
        for data in dataArr:
            func({myKey:data[myKey] for myKey in keysList},data)
    
    def updateUserInfos(self):
        insertFuncArr = [self.db.userSummary.insert_many,self.db.userOwns.insert_many,self.db.userAchieve.insert_many,self.db.userBan.insert_many]
        replaceFuncArr = [self.db.userSummary.replace_one,self.db.userOwns.replace_one,self.db.userAchieve.replace_one,self.db.userBan.replace_one]
        updateKeys = [{"steamid":-1},{"steamid":-1},{"steamid":-1,"appid":self.currentAppId},{"steamid":-1,"appid":self.currentAppId}]
        users_inDB = set(self.db.userSummary.distinct("steamid"))
        newUsers = self.authors - users_inDB
        replaceUser = users_inDB & self.authors
        
        newUsersC = list(self.chunks(list(newUsers), 100))
        replaceUserC = list(self.chunks(list(replaceUser), 100))
        
        for taskflag,userChunks,funcArr in [("insert",newUsersC,insertFuncArr),("replace",replaceUserC,replaceFuncArr)] :
            for userlist in userChunks:
                us = self.getUserSummary(userlist) # pk : steamid
                uo = self.getUserOwns(userlist)    # pk : steamid
                ua = self.getUserAchieve(userlist) # pk : steamid + appid
                ub = self.getUserBan(userlist)     # pk : steamid + appid
                for dataArr,func,updateKey in zip([us,uo,ua,ub],funcArr,updateKeys):
                    if len(dataArr) == 0:
                        continue
                    if taskflag == "insert":
                        func(dataArr)
                    else:
                        self.replaceUserCollection(arr,func,updateKey)
                    
        return ;





