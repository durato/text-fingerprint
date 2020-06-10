import collections, concurrent.futures, time, sys, pickle
from multiprocessing import Process, Manager, Pool, Array
from . import utils

##############################
#### PROCESS RAW TEXT ########
##############################

def consume(lines, printed=False, maxDistance=5):
    connections = []
    ignored = ["!", ".", ",", "?", ";", "&gt", "&lt"]
    for line in lines:
        lastWords = collections.deque(maxDistance*[""], maxDistance)
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
            for i in range(maxDistance):
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
def aggregateWorker(args):
    d, existing, limit, printed = args
    aggregate(d, existing, limit, printed)

def exit_handler():
    print("finished process:"+str(os.getpid()))

def aggregate(d, existing = None, limit=0, printed=False, maxDistance=5):
    # from multiprocessing.util import Finalize
    consumed_data = list(d)
    if existing is not None:
        consumed_data = consumed_data + existing

    pctStr = ""
    consumed_data = sorted(consumed_data, key=lambda x:(x[0],x[1],x[2]))

    if len(consumed_data)==0:
        if printed:
            print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"Tried to aggregate empty list")
        return []

    if limit>0 and len(consumed_data)>limit:
        if printed:
            print(time.strftime('%Y-%m-%d %H:%M:%S | ')+f"{len(consumed_data)=}, Splitting.")
        firstHalf, lastHalf = utils.split_list(consumed_data)

        with concurrent.futures.ProcessPoolExecutor() as executor:
            processes = []
            for dist in range(maxDistance+1):
                print(time.strftime('%Y-%m-%d %H:%M:%S | ')+f'{dist=}')
                processes.append(executor.submit(aggregate, list(filter(lambda x: x[2]==dist, consumed_data)), None, 0, True, maxDistance))

            processes.append(executor.submit(aggregate, list(filter(lambda x: x[2] not in range(maxDistance+1), consumed_data)), None, 0, True, maxDistance))

            result = []
            if printed:
                print(time.strftime('%Y-%m-%d %H:%M:%S | ')+f'{len(processes)=} ||')
            for proc in processes:
                result += proc.result()

            return result
            # Finalize(limit, exit_handler, exitpriority=0)

    result = []
    if printed:
        print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"Aggregating "+str(len(consumed_data))+" entries. ")
    originalCount = len(consumed_data)
    consumed_data = sorted(consumed_data, key=lambda x:(x[0],x[1],x[2]))
    percent = 0.0

    while len(consumed_data)>0:
        currentCount = len(consumed_data)

        try:
            percent = 1-(currentCount / originalCount)
        except:
            if printed:
                print(str(percent*100)+"%")
            raise
        pctStrCur = utils.formatPercents([percent])

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
        print(time.strftime('%Y-%m-%d %H:%M:%S | ')+f'Aggregated. {len(result)=}')
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
    print(time.strftime('%Y-%m-%d %H:%M:%S | ')+"Started: ")
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

if __name__ == '__main__' or __name__ == 'processing':
    freeze_support()
