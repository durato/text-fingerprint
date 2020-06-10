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
