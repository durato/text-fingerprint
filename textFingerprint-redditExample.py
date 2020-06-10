import textFingerprint as tfp
from textFingerprint import redditComments as tfpreddit

##############################
#### MAIN ####################
##############################
data=[]

#cleanseRawData("reddit_comments_dbAggrData_fromQuery20200316_171916.dat")
#exit()

tfpreddit.readFromRedditCommentDb(data, lastDate=1430441403) #cqui4sx
