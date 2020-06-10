import collections, concurrent.futures, time, sys, pickle
from . import utils

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
        line = utils.punctualize(line)
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
        firstHalf, lastHalf = utils.split_list(consumed_data)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            percents.append(float(0))
            percents.append(float(0))
            t1 = executor.submit(aggregate, firstHalf, None, len(percents)-2, percents, limit)
            t2 = executor.submit(aggregate, lastHalf, None, len(percents)-1, percents, limit)
            consumed_data = t1.result() + t2.result()
            # todo: https://www.youtube.com/watch?v=fKl2JW_qrso * Python Multiprocessing Tutorial: Run Code in Parallel Using the Multiprocessing Module

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
        pctStrCur = utils.formatPercents(percents)

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

def cleanseRawData(fileName):
    rawData = utils.dataSort(loadExistingBinaryData(fileName)[:10000])
    cleansed = utils.dataSort(aggregate(rawData, printed=True))
    writeBinaryData(fileName+".cleansed.dat", cleansed)

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

    data = utils.dataSort(data)

    with open(outfname+".dat", mode="wb") as outf:
        pickle.dump(data, outf)

    print(utils.dataSort(data)[0:100])
    return data

##############################
#### preprocess Short Text ###
##############################
def preprocessShortText(lines):
    assert (lines is not None and len(lines)>0)

    inputData = aggregate(consume(lines))
    inputData = utils.dataSort(inputData, col=2)
    return inputData
