import sqlite3, pickle, pyodbc

def filter_nonprintable(text):
    import string
    # Get the difference of all ASCII characters from the set of printable characters
    nonprintable = set(list([chr(i) for i in range(128)])+['\t','\n']).difference(string.printable)
    # Use translate to remove all non-printable characters
    return text.translate({ord(character):None for character in nonprintable})

data = []
dbFile = "reddit_comments_dbAggrData20200311_193824.dat.sqlite"

with open("reddit_comments_dbAggrData20200311_193824.dat", "rb") as f:
    data = pickle.load(f)
    #data = list(filter(lambda x: [filter_nonprintable(x[0]),filter_nonprintable(x[1]),x[2],x[3]], data[:10]))

with sqlite3.connect(dbFile) as conn:
    q = """CREATE TABLE IF NOT EXISTS [Connections] (
        [Id] INTEGER NOT NULL PRIMARY KEY,
        [Word1] NVARCHAR(255) NOT NULL,
        [Word2] NVARCHAR(255) NOT NULL,
        [Distance] INTEGER NOT NULL,
        [Occurrence] INTEGER NOT NULL
    )"""

    c = conn.cursor()
    c.execute(q)

    q = """SELECT COUNT(*) FROM Connections"""
    count = c.execute(q).fetchone()[0]
    print("Row count in table [Connections]:", count)
    if count==0:
        print("Inserting...")
        pq = """INSERT INTO Connections VALUES(NULL,?,?,?,?)"""
        print(data[0])
        c.executemany(pq, data)

#with pyodbc.connect('DRIVER={SQL Server};SERVER=.\\SQLEXPRESS;DATABASE=RedditComments;Trusted_Connection=yes') as conn:
with pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=.\\SQLEXPRESS;DATABASE=RedditComments;Trusted_Connection=yes') as conn:
    c = conn.cursor()
    c.fast_executemany = True
    c.execute(q)

    q = """SELECT COUNT(*) FROM Connections"""
    count = c.execute(q).fetchone()[0]
    print("Row count in table [Connections]:", count)
    if count==0:
        print("Inserting...")
        pq = """INSERT INTO Connections VALUES(?,?,?,?)"""
        index = 0
        page = 10000
        print("len:",len(data))
        try:
            print("Starting bulk insert")
            while index+page < len(data):

                #c.executemany(pq, data[index:index+page])
                c.executemany(pq, list(filter(lambda x: [filter_nonprintable(x[0]),filter_nonprintable(x[1]),x[2],x[3]], data[index:index+page]))) #x[4] is commentID
                c.commit()
                print(str(page)+" rows inserted. Last:", index+page, "All:", len(data))
                index+=page

            if index<len(data):
                c.executemany(pq, data[index:])
            #conn.commit()
        except:
            print(data[index:index+page], "Error")
            raise
