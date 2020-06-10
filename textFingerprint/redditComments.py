import os,io,pickle,time,sqlite3,string
from . import processing, utils

def flushStatusData(lastdate, lastcomment):
    print("Last comment ID parsed: ", lastcomment)
    print("Last date parsed: ", lastdate)
    with open("lastParsed"+time.strftime('%Y%m%d_%H%M%S')+".txt", mode="w") as outf:
        outf.write(str(lastdate)+"\n")
        outf.write(lastcomment+"\n")

def readFromRedditCommentDb(data, lastDate=0, rowcount=50000):
    dbfile = "K:\\DATA\\RedditCommentsMay2015\\reddit-comments-may-2015\\database.sqlite" #https://www.kaggle.com/reddit/reddit-comments-may-2015
    dbLines = []
    dbAggrData = []

    with sqlite3.connect(dbfile) as conn:
        conn.text_factory = lambda b: b.decode(errors = 'ignore')#lambda x: str(x, 'utf-8')
        c = conn.cursor()
        schema = c.execute("""PRAGMA table_info(May2015)""")
        #print(schema.fetchall())
        consume_at=100
        aggregate_at=2000

        queryText = """SELECT body, id, created_utc
        FROM May2015
        WHERE LENGTH(body)>50 AND NOT (body LIKE '[deleted]')"""+" AND created_utc>"+str(lastDate)+" ORDER BY created_utc ASC LIMIT "+str(rowcount)

        queryResult = c.execute(queryText)
        #time.sleep(10)
        processedCount = 0
        processCycles = 0
        lastCommentId = "-"

        for x in queryResult:
            # print(x)
            # time.sleep(10)
            try:
                dbLines.append(x[0].split('\n'))
                processedCount += 1
                lastCommentId = x[1]
                lastCommentDate = x[2]

                if processedCount==aggregate_at:
                    processedCount = 0
                    processCycles += 1
                    print(". Connections:", len(dbAggrData))
                    print(time.strftime('%Y-%m-%d %H:%M:%S | ')+str(aggregate_at)+" comments processed.", "Total:",str(processCycles*aggregate_at))

                    dbAggrDataLength = len(dbAggrData)
                    data = data + processing.aggregate(dbAggrData, printed=True, limit=100000)
                    dbAggrData=[]

                    print(time.strftime('%Y-%m-%d %H:%M:%S | '),"Processing cycle #"+str(processCycles),"data length:",len(data))

                    if processCycles%5==0:
                        print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"Aggregating data after 5 cycles")
                        data = processing.aggregate(data, printed=True, limit=500000)

                    #in case something funny happens
                    utils.writeBinaryData("reddit_comments_dbAggrData_fromQuery"+time.strftime('%Y%m%d_%H%M%S')+".dat", data)

                    flushStatusData(lastCommentDate, lastCommentId)

                    print("=======================================")
                    print("=======================================")

                elif processedCount%(10*consume_at)==0:
                    print(". Connections:", len(dbAggrData))
                elif processedCount%consume_at==0:
                    print(".", end="", flush=True)
                    for line in dbLines:
                        dbAggrData = dbAggrData + processing.consume(line)
                    dbLines = []

            except KeyboardInterrupt:
                flushStatusData(lastCommentDate, lastCommentId)
                raise
            except AssertionError:
                flushStatusData(lastCommentDate, lastCommentId)
                raise
            except Exception as ex:
                print(f'error: {ex=}')
                flushStatusData(lastCommentDate, lastCommentId)
                continue


    if len(dbLines)>0:
        print(time.strftime('%Y-%m-%d %H:%M:%S | '),len(dbLines),"lines remaining in dbLines, aggregating that")
        for line in dbLines:
            dbAggrData = dbAggrData + processing.preprocessShortText(line)
        dbLines = []
        data = data + processing.aggregate(dbAggrData, printed=True, limit=100000)
        dbAggrData=[]

    print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"Last comment ID parsed: ", lastCommentId)

    #in case something funny happens
    with open("reddit_comments_dbAggrData"+time.strftime('%Y%m%d_%H%M%S')+".dat", mode="wb") as outf:
        pickle.dump(data, outf)

    print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"AGGREGATING EVERYTHING")
    data = processing.aggregate(data, printed=True, limit=10000)

    print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"Writing to file")
    with open("reddit_comments_data"+".dat", mode="wb") as outf:
        pickle.dump(data, outf)
