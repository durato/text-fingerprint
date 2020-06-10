

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





#cleanseRawData("reddit_comments_dbAggrData_fromQuery20200316_171916.dat")
#exit()

# 2020-03-17 00:35:30 |  Processing cycle #19 data length: 4039063
# Writing 4039063 entries to file
# Last comment ID parsed:
# Last date parsed:



################################################################################
################################################################################
readFromRedditCommentDb(data, lastDate=1430441403) #cqui4sx
