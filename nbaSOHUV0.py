import re
import pymongo
import urllib.request as hr
import datetime, time

livePattern = re.compile('>(\d{2}):(\d{1,2}|\.\d).+>(\d{1,3}-\d{1,3})')
awayPattern = re.compile('客队：(.+)<')
homePattern = re.compile('主队：(.+)<')
startDateStr = '20101031'
endDateStr = '20171030'
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

startDate = datetime.date(int(startDateStr[:4]), int(startDateStr[4:6]), int(startDateStr[6:]))
endDate = datetime.date(int(endDateStr[:4]), int(endDateStr[4:6]), int(endDateStr[6:]))
while startDate <= endDate:
    currentDate = endDate.strftime('%Y%m%d')
    for i in range(100):
        home = ""
        away = ""
        liveScores = []
        httpUrl = "http://data.sports.sohu.com/nba/live_html/nba_live_" + currentDate + \
                  str(i).zfill(2) + ".htm"
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
        client = pymongo.MongoClient('localhost', 27017)
        db = client['nba']
        coll = db['game']
        coll.insert_many(gameScores, ordered=True)
        client.close()
        print("mongodb inserted.")
        gameScores = []
print("%s games downloaded." % len(gameScores))
# insert to mongodb.
client = pymongo.MongoClient('localhost', 27017)
db = client['nba']
coll = db['game']
coll.insert_many(gameScores, ordered=True)
client.close()
print("mongodb inserted.")

