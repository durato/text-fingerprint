import textFingerprint as tfp
from textFingerprint import redditComments as tfpreddit
from multiprocessing import *

##############################
#### MAIN ####################
##############################
def main():
    data=[]

    #cleanseRawData("reddit_comments_dbAggrData_fromQuery20200316_171916.dat")
    #exit()

    tfpreddit.readFromRedditCommentDb(data, lastDate=0, rowcount=50000) #cqui4sx

if __name__ == '__main__':
    freeze_support()
    main()
