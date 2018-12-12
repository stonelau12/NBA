import re
import pymongo
import urllib.request as hr
import datetime, time
import numpy as np
import pickle

#livePattern = re.compile('\((\d{2}):(\d{1,2}|\.\d)\).+>(\d{1,3}-\d{1,3})')
livePattern = re.compile('>(\d{2}):(\d{1,2}|\.\d).+>(\d{1,3}-\d{1,3})')
awayPattern = re.compile('客队：(.+)<')
homePattern = re.compile('主队：(.+)<')
linkPattern = re.compile('<a href="(live_html/nba_live_\d{10}\.htm)" target="_blank">比分直播</a>')
DB = 'nba'
COLL = 'game'
BKNAME = 'F:\\mongodb\\nbascores.pkl'
startDateStr = '20181123'
endDateStr = '20181123'

gameScores = []


def updateScorePerMinute(liveLine):
    scorePerMinute = []
    currentMinute = 0
    lastMinute = 12
    lastScore = scores = aScore = bScore = 0
    gameTimeIndex = 0
    for line in liveLine:
        currentMinute = int(line[0])
        vsScores = line[2].split('-')
        aScore = int(vsScores[0])
        bScore = int(vsScores[1])
        if lastMinute == currentMinute:
            scores = bScore + aScore
        else:
            if lastMinute != 12:
                scorePerMinute.append(scores-lastScore)
                gameTimeIndex += 1
            lastMinute = currentMinute
            lastScore = scores
    # update last minute score.
    scorePerMinute.append(scores - lastScore)
    return scorePerMinute


def getGameList(gameDate):
    gameList = []
    sohuNbaUrl = "http://data.sports.sohu.com/nba/nba_schedule_by_day.php?d=" + gameDate
    try:
        sohuByDay = hr.urlopen(sohuNbaUrl, timeout=20)
        if sohuByDay.status == 200:
            sohuByDayData = sohuByDay.read().decode('gbk')
            mat = linkPattern.findall(sohuByDayData)
            if mat:
                gameList = mat
    except:
        pass
    return gameList


def insertToMongo(db, coll, gameData):
    try:
        client = pymongo.MongoClient('localhost', 27017)
        db = client[db]
        coll = db[coll]
        coll.insert_many(gameData, ordered=True)
        client.close()
        print("mongodb inserted.")
    except:
        print("Insert to MongoDb failured.")


def getScorePerHQ(db, coll, fileName):
    client = pymongo.MongoClient('localhost', 27017)
    db = client[db]
    coll = db[coll]
    gameDatas = coll.find()
    client.close()

    rawData = []
    for gameData in gameDatas:
        score = gameData["SCORES"]
        rawData.append([sum(score[:6]), sum(score[6:12]), sum(score[12:18]), sum(score[18:24]),
                        sum(score[24:30]), sum(score[30:36]), sum(score[36:42]), sum(score[42:])])
    scorePerHQ = np.array(rawData)
    pickle.dump(scorePerHQ, open(fileName, 'wb'))
    print("SCORE EXPORT TO %s" % fileName)

startDate = datetime.date(int(startDateStr[:4]), int(startDateStr[4:6]), int(startDateStr[6:]))
endDate = datetime.date(int(endDateStr[:4]), int(endDateStr[4:6]), int(endDateStr[6:]))
while startDate <= endDate:
    currentDate = endDate.strftime('%Y%m%d')
    gameList = getGameList(endDate.strftime('%Y-%m-%d'))
    for gameLink in gameList:
        home = ""
        away = ""
        liveScores = []
        httpUrl = "http://data.sports.sohu.com/nba/" + gameLink
        try:
            nbaLiveWeb = hr.urlopen(httpUrl, timeout=20)
            if nbaLiveWeb.status == 200:
                nbaLiveData = nbaLiveWeb.read().decode('gbk')
                mat = homePattern.search(nbaLiveData)
                if mat:
                    home = mat.groups()[0]
                mat = awayPattern.search(nbaLiveData)
                if mat:
                    away = mat.groups()[0]
                mat = livePattern.findall(nbaLiveData)
                if mat:
                    scorePer = updateScorePerMinute(mat)
                    gameScores.append({"DATE": currentDate, "HOME": home, "AWAY": away,
                                       "SCORES": scorePer[:48]})
            #time.sleep(5)
        except:
            pass
    endDate -= datetime.timedelta(days=1)
    print('%s: %s: %s' % (datetime.datetime.now(), currentDate, len(gameScores)))
    if len(gameScores) >= 500:
        insertToMongo(DB, COLL, gameScores)
        gameScores = []
print("%s games downloaded." % len(gameScores))
# insert to mongodb.
if len(gameScores) > 0:
    insertToMongo(DB, COLL, gameScores)
getScorePerHQ(DB, COLL, BKNAME)


