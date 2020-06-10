import sys, pickle, random

data = []

with open("reddit_comments_dbAggrData_fromQuery20200317_003530.dat", "rb") as f:
    data = pickle.load(f)

data = list(filter(lambda x:"com/" not in x[0]+x[1], data))
data = sorted(data, key = lambda x:x[3], reverse=True)

print(data[0:10])


def generate(length = 100):
    s = ""

    starters = list(filter(lambda x: x[1][0].isupper(), data))
    #print(starters[0:10])
    words = []

    words.append(random.choice(starters[0:10]))

    #print(words)
    s += words[0][0] + " "
    #print(words[-1])
    #print(words[-1][1])

    while len(s)<length:
        nextwordsInit = list(filter(lambda x:x[0].lower()==words[-1][1].lower() and x[2]==1, data))
        nextwords = nextwordsInit

        if len(nextwords) == 0:
            nextwords = data
        else:
            distance = 1
            #print(nextwords[0:10])
            while distance<5 and len(words)>distance and len(nextwordsInit)>10:
                nextwords = nextwordsInit
                distance += 1
                nextwordsInit = list(filter(lambda x:x[0].lower()==words[-distance][1].lower(), nextwordsInit))

        word = random.choice(nextwords)

        if word[1] != words[-1][1]:
            words.append(word)
            s = " ".join([x[1] for x in words])

    return s

for i in range(0,10):
    print(generate())
