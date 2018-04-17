import socket
import socks
from urllib.request import urlopen
from collections import deque
from copy import copy
from bs4 import BeautifulSoup
import requests,re,datetime,sys,getopt,pymongo,time
import grequests


class SteamData:
    R_URL = "http://store.steampowered.com/appreviews/%d?json=1&filter=recent&start_offset=%d"
    GL_URL = "http://api.steampowered.com/ISteamApps/GetAppList/v0002/?key=STEAMKEY&format=json"
    G_URL = "http://store.steampowered.com/api/appdetails?appids=%d"
    U_URL = "http://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=%s&steamids=%s"
    UA_URL = "http://api.steampowered.com/ISteamUserStats/GetPlayerAchievements/v0001/?appid=%d&key=%s&steamid=%s"
    US_URL = "http://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?appid=%d&key=%s&steamid=%s"
    UO_URL = " http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=%s&steamid=%s&format=json"
    UB_URL = "http://api.steampowered.com/ISteamUser/GetPlayerBans/v1/?key=%s&steamids=%s"
    topSellerList_URL = "http://store.steampowered.com/search/?filter=topsellers&page=%d"

    APIKEY= "9F041FB9B406DCC0FD036440C6BC459C"
    authors = set()
    authorsList = []
    currentAppId = -1




    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client.steam
        self.torOff = True

    def onTorBrowser(self):
        self.torOff = False

    def pysocksSetting(self):
        if self.torOff == True:
            return 

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
        print("total app count: %d"%self.db.appList.count())

    def __getGameDetailMapper(self,x):
        try:
            return x.json()[list(x.json().keys())[0]]["data"]
        except:
            return
    def getAppDetails(self):
        curAppidsSet = set(self.getAppids())
        updatedDetailsSet = set(self.db.appDetail.distinct('steam_appid'))
        ids2Add = curAppidsSet - updatedDetailsSet
        for idsChunk in list(self.chunks(list(ids2Add), 5)):
            rs = (grequests.get(self.G_URL%(i),timeout=10) for i in idsChunk)
            myresponse = grequests.map(rs)
            time.sleep(2)
            appDetails_ = map(lambda x: self.__getGameDetailMapper(x),myresponse)
            appDetails = [i for i in appDetails_ if i != None]
            yield appDetails


    def updateAppDetail(self):
        for appDetails in self.getAppDetails():
            lengthRslt = len(appDetails)
            if lengthRslt == 0:
                print("zero app Data")
                continue
            self.db.appDetail.insert_many(appDetails)
            print("insert %d app details"%lengthRslt)




    def getTopSellerAppIds(self):
        def appIdMapper(i):
            if i == None:
                return []
            return list(map(lambda x:int(x),i.split(",")))
        for idx in range(1,74):
            res = requests.get(self.topSellerList_URL%(idx))
            restxt = res.text
            soup = BeautifulSoup(restxt,"html.parser")
            rslt = soup.find_all("a",{"class":"search_result_row"})
            appids = []
            for rslt_i in rslt:
                try:
                    for id_i in appIdMapper(rslt_i.attrs['data-ds-appid']):
                        appids.append(id_i)
                except:
                    continue
            yield appids

    def updateAppDetail2(self):
        #curAppidsSet = set(self.getAppids())
        #updatedDetailsSet = set(self.db.appDetail.distinct('steam_appid'))
        #ids2Add = curAppidsSet - updatedDetailsSet
        for aid_arr in self.getTopSellerAppIds():
            print(aid_arr)
            if len(aid_arr) == 0:
                continue
            for aid in aid_arr:
                while True:
                    res = requests.get(self.G_URL%(aid))
                    time.sleep(1)
                    myAppDetail = self.__getGameDetailMapper(res)
                    if myAppDetail == None:
                        print("get app detail fail")
                        continue
                    print("insert app Detail %d"%aid)
                    self.db.appDetail.insert(myAppDetail)
                    break


   


    def updateAllAppReviews(self):
        appids = self.getAppids()
        for i in appids:
            self.updateAppReviews(i)
            print("appid %d, insert job is complete"%i)
        return


    def reqReviewList(self,appid,rng):
        rs = (grequests.get(self.R_URL%(appid,i)) for i in rng)
        rs_a= grequests.map(rs)
        res = [j for i in rs_a if i != None for j in i.json()['reviews']]
        for res_i in res:
            try:
                res_i.update({"appid":appid})
                self.authors.add(res_i['author']['steamid'])
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
        rs = (grequests.get(self.U_URL%(self.APIKEY,steamid)) for steamid in userlist)
        t0 = time.time()
        res_ = grequests.map(rs)
        status =  [i.status_code for i in res_ if i != None]
        print("summary status",status[:5])
        print("summary req time %d"%(time.time() - t0))
        while len(status) == 0:
            self.pysocksSetting()
            rs = (grequests.get(self.U_URL%(self.APIKEY,steamid)) for steamid in userlist)
            res_ = grequests.map(rs)
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
        rs = (grequests.get(self.UO_URL%(self.APIKEY,steamid)) for steamid in userlist)
        t0 = time.time()
        res_ = grequests.map(rs)
        status =  [i.status_code for i in res_ if i != None]
        print("owns status",status[:5])
        print("owns req time %d"%(time.time() - t0))
        while len(status) == 0:
            self.pysocksSetting()
            rs = (grequests.get(self.UO_URL%(self.APIKEY,steamid)) for steamid in userlist)
            res_ = grequests.map(rs)
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
        rs = (grequests.get(self.UA_URL%(self.currentAppId,self.APIKEY,steamid)) for steamid in userlist)
        t0 = time.time()
        res_ = grequests.map(rs)
        
        status =  [i.status_code for i in res_ if i != None]
        print("achieve status",status[:5])
        print("achieve req time %d"%(time.time() - t0))
        while len(status) == 0:
            self.pysocksSetting()
            rs = (grequests.get(self.UA_URL%(self.currentAppId,self.APIKEY,steamid)) for steamid in userlist)
            res_ = grequests.map(rs)
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
        rs = (grequests.get(self.UB_URL%(self.APIKEY,steamid)) for steamid in userlist)
        t0 = time.time()
        res_ = grequests.map(rs)
        status =  [i.status_code for i in res_ if i != None]
        print("ban status",status[:5])
        print("ban req time %d"%(time.time() - t0))
        while len(status) == 0:
            self.pysocksSetting()
            rs = (grequests.get(self.UB_URL%(self.APIKEY,steamid)) for steamid in userlist)
            res_ = grequests.map(rs)
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
                print("userSummary Size %d"%self.db.userSummary.count())
                print("userOwns    Size %d"%self.db.userOwns.count())
                print("userAchieve Size %d"%self.db.userAchieve.count())
                print("userBan     Size %d"%self.db.userBan.count())

        return ;

def main():
    steamdata = SteamData()
    steamdata.updateAppReviews(578080)
            
