import socket
import socks
from urllib.request import urlopen
from collections import deque
from copy import copy
import requests,re,datetime,sys,getopt,pymongo,time
from requests_futures.sessions import FuturesSession


class SteamData:
    R_URL = "http://store.steampowered.com/appreviews/%d?json=1&filter=recent&start_offset=%d"
    GL_URL = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/?key=STEAMKEY&format=json"
    G_URL = "http://store.steampowered.com/api/appdetails?appids=%d"
    U_URL = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=%s&steamids=%s"
    UA_URL = "http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid=%d&key=%s&steamid=%s"
    US_URL = "http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid=%d&key=%s&steamid=%s"
    UO_URL = " http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=%s&steamid=%s&format=json"
    UB_URL = "http://api.steampowered.com/ISteamUser/GetPlayerBans/v1/?key=%s&steamids=%s"
    APIKEY= "9F041FB9B406DCC0FD036440C6BC459C"
    authors = set()
    authorsList = []
    currentAppId = -1




    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.steam
        self.session = FuturesSession(max_workers=100)

    def pysocksSetting(self):
        socks.set_default_proxy(socks.SOCKS5,"localhost",9150)
        socket.socket = socks.socksocket
        ipch = 'http://icanhazip.com'
        print(urlopen(ipch).read())

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
        for idsChunk in list(self.chunks(list(ids2Add), 100)):
            rs = (self.session.get(self.G_URL%(i)) for i in idsChunk)
            myresponse = map(lambda x:x.result(),rs)
            appDetails_ = map(lambda x: self.__getGameDetailMapper(x),myresponse)
            appDetails = [i for i in appDetails_ if i != None]
            yield appDetails


    def updateAppDetail(self):
        for appDetails in self.getAppDetails():
            self.db.appDetail.insert_many(appDetails)
            print("insert %d app details"%len(appDetails))

    def updateAllAppReviews(self):
        appids = self.getAppids()
        for i in appids:
            self.updateAppReviews(i)
            print("appid %d, insert job is complete"%i)
        return


    def reqReviewList(self,appid,rng):
        rs = (self.session.get(self.R_URL%(appid,i)) for i in rng)
        rs_ = list(map(lambda x:x.result(),rs))
        res = []
        for rs_i in re_:
            try:
                res_i = rs_i.json()
                res_i.update({"appid":appid})
                self.authors.add(res_i['author']['steamid'])
                res.append(res_i)
            except:
                continue
        return res

    def getAllReviews(self,appid):
        return self.db.appReview.find({"appid":appid})

    def updateAppReviews(self,appid):
        self.pysocksSetting()
        self.currentAppId = appid
        failCnt = 0
        insufficientCnt = 0
        if self.db.appReview.count({"appid":appid}) == 0:
            print("inserting reviews first time")
            n = 0
            while failCnt < 10:
                rng = range(n,(n+1)+80,20)
                res = self.reqReviewList(appid,rng)
                if len(res) == 0:
                    failCnt += 1
                    continue
                elif len(res) < 100:
                    print("insufficient data")
                    insufficientCnt += 1
                    if insufficientCnt < 10:
                        continue

                n = rng[-1]
                failCnt = 0
                insufficientCnt = 0
                self.db.appReview.insert_many(res)
                print("review size %d"%(self.db.appReview.count()),end="\r")

        else:
            print("insert additional reviews")
            lastUpdatedDt = self.db.appReview.find({"appid":appid}).sort('timestamp_created',-1)[0]['timestamp_created']
            n = 0
            isEnd = False
            inTheEnd = False
            while not isEnd:
                rng = range(n,(n+1)+80,20)
                res = self.reqReviewList(appid,rng)
                if len(res) == 0:
                    failCnt += 1
                    continue
                elif len(res) < 100:
                    print("insufficient data")
                    insufficientCnt += 1
                    if insufficientCnt < 10:
                        continue

                n = rng[-1]
                failCnt = 0
                insufficientCnt = 0

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
        f = open("reviewUsers.txt","w")
        for i in list(self.authors):
            f.write(str(i)+"\n")
        f.close()
                self.authors.clear()
        return


    def getUserSummary(self,userlist):
        rs = (self.session.get(self.U_URL%(self.APIKEY,steamid)) for steamid in userlist)
        res_ = map(lambda x:x.result(),rs)
        status =  [i.status_code for i in res_ if i != None]
        while len(status) == 0:
            self.pysocksSetting()
            rs = (self.session.get(self.U_URL%(self.APIKEY,steamid)) for steamid in userlist)
            res_ = map(lambda x:x.result(),rs)
            status = [i.status_code for i in res_ if i != None]


        docs = []
        for i,steamid_i in zip(res_,userlist):
            try:
                doc = i.json()["response"]['players'][0]
            except:
                continue
            docs.append(doc)
        return docs


    def getUserOwns(self,userlist):
        rs = (self.session.get(self.UO_URL%(self.APIKEY,steamid)) for steamid in userlist)
        res_ = map(lambda x:x.result(),rs)
        status =  [i.status_code for i in res_ if i != None]
        while 200*len(status) != sum(status) or len(status) == 0:
            self.pysocksSetting()
            rs = (self.session.get(self.UO_URL%(self.APIKEY,steamid)) for steamid in userlist)
            res_ = map(lambda x:x.result(),rs)
            status = [i.status_code for i in res_ if i != None]

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
        rs = (self.session.get(self.UA_URL%(self.currentAppId,self.APIKEY,steamid)) for steamid in userlist)
        res_ = map(lambda x:x.result(),rs)
        status =  [i.status_code for i in res_ if i != None]
        while 200*len(status) != sum(status) or len(status) == 0:
            self.pysocksSetting()
            rs = (self.session.get(self.UA_URL%(self.currentAppId,self.APIKEY,steamid)) for steamid in userlist)
            res_ = map(lambda x:x.result(),rs)
            status = [i.status_code for i in res_ if i != None]
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
        rs = (self.session.get(self.UB_URL%(self.APIKEY,steamid)) for steamid in userlist)
        res_ = map(lambda x:x.result(),rs)
        status =  [i.status_code for i in res_ if i != None]
        while 200*len(status) != sum(status) or len(status) == 0:
            self.pysocksSetting()
            rs = (self.session.get(self.UB_URL%(self.APIKEY,steamid)) for steamid in userlist)
            res_ = map(lambda x:x.result(),rs)
            status = [i.status_code for i in res_ if i != None]
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
        self.pysocksSetting()
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
                        self.replaceUserCollection(dataArr,func,updateKey)

        return ;

def main():
    steamdata = SteamData()
    steamdata.updateAppReviews(578080)
            
