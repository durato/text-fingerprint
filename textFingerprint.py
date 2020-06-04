# -*- coding: utf-8 -*-

import sys,os,collections,io,concurrent.futures,pickle,time,re,math,statistics,sqlite3,string
from shutil import copyfile

# class FuncThread(threading.Thread):
#     def __init__(self, target, *args):
#         self._target = target
#         self._args = args
#         threading.Thread.__init__(self)
#
#     def run(self):
#         self._target(*self._args)

def split_list(a_list):
    half = len(a_list)//2
    return a_list[:half], a_list[half:]

# Get the difference of all ASCII characters from the set of printable characters
nonprintable = set(list([chr(i) for i in range(128)])).difference(string.printable).union(['\n','\t','\r',chr(13)])
#print(sorted([ord(x) for x in nonprintable]))
#exit()

def filter_nonprintable(text):
    # Use translate to remove all non-printable characters
    return text.translate({ord(character):None for character in nonprintable})

def punctualize(line):
    words = line.split(" ")
    punct = []
    for word in words:
        if len(word)==0:
            continue
        wr = filter_nonprintable(word).replace("\"","").replace("\'","").replace("`","").replace(".", " . ").replace(",", " , ").replace(":", " : ").replace(";", " ; ").replace("-", " - ").replace("?", " ? ").replace("!", " ! ").replace(")"," ").replace("("," ").replace("<"," ").replace(">"," ").replace("  "," ")
        if len(word)-len(wr)<4 and len(wr)>0: # filter too rubbish
            punct.append(wr)
        # else:
        #     print("RUBBISH FAILED", word, wr, len(wr), len(word))
        #     time.sleep(1)

    return ' '.join(punct).strip()

def formatPercents(percents):
    ps = sorted(percents, reverse=True)[0:8]
    result = ""
    for i in range(0,len(ps)):
        p = ps[i]
        if p==0 or p==1:
            continue
        pStr = str(i)+"|"+str("%.0f%%" % (100*p))
        result = result + pStr + " "
    return result

def dataSort(data, col=3):
    return sorted(data, key=lambda x:x[col], reverse=True)

##############################
#### PROCESS RAW TEXT ########
##############################

def consume(lines, printed=False):
    connections = []
    ignored = ["!", ".", ",", "?", ";", "&gt", "&lt"]
    for line in lines:
        lastWords = collections.deque(5*[""], 5)
        #print(line)
        #print(punctualize(line))
        line = punctualize(line)
        #print(line)
        for word in line.split(" "):
            if word=="" or word==" " or (word in ignored):
                continue
            # if "Another" in word:
            #     print(word, list([ord(x) for x in word]))
            #     word="YEEHAA"+word
            #     #print(line, list([ord(x) for x in line]))
            #     time.sleep(1)
            #if lastWords is not None:
            connections.append([word, word, 0, 1]) # word count, simply.
            #connections.append([lastWord, word, 1, 1])
            for i in range(0, len(lastWords)):
                if lastWords[i] != "" and lastWords[i] != " ":
                    connections.append([lastWords[i], word, i+1, 1])
                    #print(word, [lastWords[i], word, i+1, 1])
            lastWords.appendleft(word)
            #print(lastWords)

    if printed:
        print(str(len(lines))+" lines consumed.")
    return list(filter(lambda x: x[0]!="" and x[1]!="", connections))

##############################
#### AGGREGATION #############
##############################

def aggregate(d, existing = None, slot=0, percents=[float(0)], limit=0, printed=False):
    if printed:
        print("Aggregating... slot=", slot)
    consumed_data = list(d)
    if existing is not None:
        consumed_data = consumed_data + existing

    pctStr = ""
    consumed_data = sorted(consumed_data, key=lambda x:x[0])

    if limit>0 and len(consumed_data)>limit:
        if printed:
            print("Splitting.")
        firstHalf, lastHalf = split_list(consumed_data)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            percents.append(float(0))
            percents.append(float(0))
            t1 = executor.submit(aggregate, firstHalf, None, len(percents)-2, percents, limit)
            t2 = executor.submit(aggregate, lastHalf, None, len(percents)-1, percents, limit)
            consumed_data = t1.result() + t2.result()

    result = []
    if printed:
        print("Aggregating "+str(len(consumed_data))+" entries. "+time.strftime('%Y-%m-%d %H:%M:%S'))
    originalCount = len(consumed_data)
    consumed_data = sorted(consumed_data, key=lambda x:x[0])

    while len(consumed_data)>0:
        currentCount = len(consumed_data)

        try:
            percents[slot] = 1-(currentCount / originalCount)
        except:
            if printed:
                print(percents, "slot = ",slot)
            raise
        pctStrCur = formatPercents(percents)

        if pctStr != pctStrCur and printed:
            print(pctStrCur, originalCount-currentCount, "/", originalCount, end="\r", flush=True)
            pctStr = pctStrCur
        sys.stdout.flush()
        agg = consumed_data[0]
        indices = []

        for i in range(1, len(consumed_data)):
            if consumed_data[i][0]>agg[0]:
                break
            if consumed_data[i][0:3] == agg[0:3]:
                agg[3] = agg[3] + consumed_data[i][3]
                indices.append(i)

        for index in reversed(indices):
            del consumed_data[index]

        del consumed_data[0]

        result.append(agg)
    if printed:
        print("Aggregated. "+time.strftime('%Y-%m-%d %H:%M:%S'))
    return result

##############################
#### LEARN ###################
##############################

def learn(fname, existingFname="", outfname="output_data.dat"):
    #fnames = ["english-sample.txt","PrideAndPrejudice.txt"]
    #fnames = ["bigfile.txt"]
    #existingFname = "output_data"
    #data = consume(["This is a good day to die.","Power level is above nine thousand!"])
    data = []
    print("Started: "+time.strftime('%Y-%m-%d %H:%M:%S'))
    if existingFname!="":
        try:
            timestr = time.strftime("%Y%m%d_%H%M%S")
            with open(existingFname+".dat", mode="rb") as f:
                data = data + pickle.load(f)

            if fname is None or fname=="":
                print("No consumable file detected for learning.")
                return data
            else:
                copyfile(existingFname+".dat", existingFname+"_"+timestr+".dat")

        except:
            ex_type, ex, tb = sys.exc_info()
            print("input file error",ex)

    try:
        with open(fname, mode="r", encoding="utf-8") as f:
            lines = f.readlines()
            consumed = consume(lines)
            data = aggregate(consumed, data, limit=0)
            #print(data)
            print(len(consumed), len(data))
            #print(sorted([(x[0],x[1],x[2]) if (x[0] is not None) else ("",x[1],x[2]) for x in data] ))
    except:
        print("Error reading learning sample.")
        exit()

    data = dataSort(data)

    with open(outfname+".dat", mode="wb") as outf:
        pickle.dump(data, outf)

    print(dataSort(data)[0:100])
    return data

##############################
#### preprocess Short Text ###
##############################
def preprocessShortText(lines):
    assert (lines is not None and len(lines)>0)

    inputData = aggregate(consume(lines))
    inputData = dataSort(inputData, col=2)
    return inputData

def connectionRank(connection):
    return pow(1.1,5-connection[3]) + pow(1.1, connection[3])

def dataRank(data):
    sum = 0
    for conn in data:
        sum += connectionRank(conn)
    print("sum", sum, len(data))
    return sum/len(data)

def truncateData(data):
    assert data is not None
    return set([(x[0], x[1], x[2]) for x in data])

def filterTrunc(data, distance):
    assert distance>=0
    return sorted([(x[0],x[1],x[2]) for x in filter(lambda x: x[2]==distance, data)])

def compareWords(first, second, standard = None, distance = 0, threshold = 3, printed=False):
    dataFirst = dataSort(filter(lambda x: x[2]==distance, first))
    dataSecond = dataSort(filter(lambda x: x[2]==distance, second))

    both = []
    onlyfirst = []
    onlySecond = []

    firstTrunc = truncateData(dataFirst)
    secondTrunc = truncateData(dataSecond)

    if standard is not None:
        stf = list(filter(lambda x: x[2]==distance, standard))
        #print("stf", stf[:30])
        #print(len(stf), math.log10(len(stf)))
        standardTrunc = truncateData(filter(lambda x: x[3]>math.log10(len(stf))-threshold, stf))
        firstTrunc = firstTrunc.difference(standardTrunc)
        secondTrunc = secondTrunc.difference(standardTrunc)

    both = filterTrunc(firstTrunc.intersection(secondTrunc), distance)
    onlyFirst = filterTrunc(firstTrunc.difference(secondTrunc), distance)
    onlySecond = filterTrunc(secondTrunc.difference(firstTrunc), distance)

    if printed:
        print("both", both)
        print("only first", onlyFirst)
        print("only second", onlySecond)
        print("1st rank:", dataRank(dataFirst))
        print("2nd rank:", dataRank(dataSecond))

    return (both, onlyFirst, onlySecond)

def evaluateOccurrences(data):
    ds = dataSort(data)
    d1, d2 = split_list(ds)
    median = d1[-1][3]
    avg = statistics.mean([x[3] for x in ds])
    #print(len(data), len(d1), d1[-1], d1[0], "avg="+str(avg), "median="+str(median))
    return (list(filter(lambda x: x[3]>avg, d1)), list(filter(lambda x: x[3]<=median, d2)))

def compare(first, second, standard=None, printed=False):
    fstOften, fstRare = evaluateOccurrences(first)
    sndOften, sndRare = evaluateOccurrences(second)

    limit=10 #for printing

    standardOften, standardRare = (None, None)

    if standard is not None and len(standard)>0:
        standardOften, standardRare = evaluateOccurrences(standard)

    result = []
    for item in [[fstOften, sndOften, "frequent", standardOften], [fstRare, sndRare, "rare", standardRare]]:
        result.append(item[0])
        result.append(item[1])

        if printed:
            print("=======================================================")
            print(item[2], "in first:\n", list(item[0])[:limit])
            print("=======================================================")
            print(item[2], "in second:\n", list(item[1])[:limit])

        for distance in range(0,2):
            both, onlyFirst, onlySecond = compareWords(item[0], item[1], standard=item[3], distance=distance)
            result.append(both)
            result.append(onlyFirst)
            result.append(onlySecond)

            if printed:
                print("=======================================================")
                print("D["+str(distance)+"] uniquely "+item[2], "in both:\n", list(both)[:limit])
                print("=======================================================")
                print("D["+str(distance)+"] uniquely "+item[2], "in only First:\n", list(onlyFirst)[:limit])
                print("=======================================================")
                print("D["+str(distance)+"] uniquely "+item[2], "in only Second:\n", list(onlySecond)[:limit])
                print("=======================================================")

    return result

##############################
#### MAIN ####################
##############################
data=[]

# textData1 = preprocessShortText(sampleInput1.split('\n'))
# textData2 = preprocessShortText(sampleInput2.split('\n'))
# textDatas = []
#
# texts = ["A Little Moody - JackPotr.txt", "The Lost Ones - little.acatalepsy.txt"]
#
# for text in texts:
#     with open(text, "r") as f:
#         if os.path.exists(text+".dat"):
#             with open(text+".dat", mode="rb") as outf:
#                 textDatas.append(pickle.load(outf))
#         else:
#             d = preprocessShortText(f.readlines())
#             textDatas.append(d)
#             with open(text+".dat", mode="wb") as outf:
#                 pickle.dump(d, outf)
#
# compare(textDatas[0], textDatas[1], standard = data, printed=True)
#compareWords(first, second, standard)

def loadExistingBinaryData(fileName):
    try:
        with open(fileName, mode="rb") as f:
            return pickle.load(f)
    except:
        print("Error loading existing binary data.")
        raise

def writeBinaryData(fileName, data):
    print("Writing", len(data), "entries to file")
    try:
        with open(fileName, mode="wb") as outf:
            return pickle.dump(data, outf)
    except:
        print("Error writing new binary data.")
        raise

def cleanseRawData(fileName):
    rawData = dataSort(loadExistingBinaryData(fileName)[:10000])
    cleansed = dataSort(aggregate(rawData, printed=True))
    writeBinaryData(fileName+".cleansed.dat", cleansed)

#cleanseRawData("reddit_comments_dbAggrData_fromQuery20200316_171916.dat")
#exit()

# 2020-03-17 00:35:30 |  Processing cycle #19 data length: 4039063
# Writing 4039063 entries to file
# Last comment ID parsed:
# Last date parsed:

def readFromRedditCommentDb(data, lastDate=0):
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
        WHERE LENGTH(body)>50 AND NOT (body LIKE '[deleted]')"""+" AND created_utc>"+str(lastDate)+" ORDER BY created_utc ASC LIMIT 50000"

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
                    data = data + aggregate(dbAggrData, printed=True)
                    dbAggrData=[]

                    print(time.strftime('%Y-%m-%d %H:%M:%S | '),"Processing cycle #"+str(processCycles),"data length:",len(data))

                    if processCycles%5==0:
                        print("Aggregating entire data")
                        data = aggregate(data, printed=True)

                    #in case something funny happens
                    writeBinaryData("reddit_comments_dbAggrData_fromQuery"+time.strftime('%Y%m%d_%H%M%S')+".dat", data)

                    print("Last comment ID parsed: ", lastCommentId)
                    print("Last date parsed: ", lastCommentDate)

                    print("=======================================")
                    print("=======================================")

                elif processedCount%(10*consume_at)==0:
                    print(". Connections:", len(dbAggrData))
                elif processedCount%consume_at==0:
                    print(".", end="", flush=True)
                    for line in dbLines:
                        dbAggrData = dbAggrData + consume(line)
                    dbLines = []

            except KeyboardInterrupt:
                print("Last comment ID parsed: ", lastCommentId)
                print("Last date parsed: ", lastCommentDate)
                raise
            except AssertionError:
                print("Last comment ID parsed: ", lastCommentId)
                print("Last date parsed: ", lastCommentDate)
                raise
            except:
                print("error")
                print("Last comment ID parsed: ", lastCommentId)
                print("Last date parsed: ", lastCommentDate)
                continue


    if len(dbLines)>0:
        print(time.strftime('%Y-%m-%d %H:%M:%S | '),len(dbLines),"lines remaining in dbLines, aggregating that")
        for line in dbLines:
            dbAggrData = dbAggrData + preprocessShortText(line)
        dbLines = []
        data = data + aggregate(dbAggrData, printed=True)
        dbAggrData=[]

    print("Last comment ID parsed: ", lastCommentId)

    #in case something funny happens
    with open("reddit_comments_dbAggrData"+time.strftime('%Y%m%d_%H%M%S')+".dat", mode="wb") as outf:
        pickle.dump(data, outf)

    print("AGGREGATING EVERYTHING")
    data = aggregate(data, printed=True)

    with open("reddit_comments_data"+".dat", mode="wb") as outf:
        pickle.dump(data, outf)

################################################################################
################################################################################
readFromRedditCommentDb(data)
