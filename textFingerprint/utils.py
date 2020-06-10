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
