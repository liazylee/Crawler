import mysql.connector
import hashlib
from mysql.connector import errorcode

class CrawlDatabaseManager:
    DB_NAME='mfw_pro_crawl'
    SERVER_IP='localhost'
    TABLES={}
    #creat new table,using sql

    TABLES['urls']=("CREATE TABLE `urls` ("
        "  `index` int(11) NOT NULL AUTO_INCREMENT," # index of queue
        "  `url` varchar(512) NOT NULL,"
        "  `md5` varchar(16) NOT NULL,"
        "  `status` varchar(11) NOT NULL DEFAULT 'new'," # could be new, downloading and finish
        "  `depth` int(11) NOT NULL,"
        "  `queue_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        "  `done_time` timestamp NOT NULL DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,"
        "  PRIMARY KEY (`index`),"
        "  UNIQUE KEY `md5` (`md5`)"
        ") ENGINE=InnoDB")

    def __init__(self,max_num_thread):
        #connect mysql server
        try:
            cnx=mysql.connector.connect(host=self.SERVER_IP,user='root')
        except mysql.connector.Error as err:
            if err.errno==errorcode.ER_ACCESS_DENDING_ERROR:
                print "Something is wrong with your user name or password"
            elif err.errno==errorcode.ER_BAD_DB_ERROR:
                print "Dateabase does not exist"
            else:
                print 'Creat Error '+err.msg
            exit(1)

        cursor =cnx.cursor()

        try:
            cnx.database= self.DB_NAME
        except mysql.connector.Error as err:
            if err.errno==errorcode.ER_ACCESS_DENDING_ERROR:
                self.create_database(cursor)
                cnx.database =self.DB_NAME
                self.create_tables(cursor)
            else:
                print err
                exit(1)

        finally:
            cursor.close()
            cnx.close()

        dbconfig={
            "database":self.DB_NAME,
            "user":"root",
            "host":self.SERVER_IP,

        }
        self.cncpool=mysql.connector.pooling.MySQLConnectionPool(pool_name='mypool',
                                                                 pool_size=max_num_thread,
                                                                 **dbconfig)

        #creat database
        def create_database(self,cursor):
            try:
                cursor.execute(
                    "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.DB_NAME))

            except mysql.connector.Error as err:
                print "Faile creating database:{}".format(err)
                exit(1)

        def creat_tables(self,cursor):
            for name ,ddl in self.TABLES.iteritems():
                try:
                    cursor.execute(ddl)

                except mysql.connector.Error as err:
                    if err.errno== errorcode.ER_TABLES_EXISTS_ERROR:
                        print 'creat tables error ALREADY EXTSTS'
                    else:
                        print 'creat tables error ' + err.msg

                else:
                    print 'Table Created'

            #put an url into queue
    def enqueueUrl(self,url,depth):
        con = self.cncpool.get_connection()
        cursor= con.cursor()
        try:
            add_url=("INSERT INTO urls (url, md5, depth) VALUES (%s, %s, %s)")
            data_url=(url ,hashlib.md5(url).hexdigest(),depth)
            cursor.execute(add_url,data_url)
            con.commit()
        except mysql.connector.Error as err:
            # print 'enqueueUrl() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()

    #get an url from queue
    def dequeueUrl(self):
        con = self.cncpool.get_connection()
        cursor =con.cursor(dictionary = True)
        try:
            #usr selct * from updata to lock the rows for read
            query=("SELECT  `index`, `url`, `depth` FROM urls WHERE status='new' ORDER BY `index` ASC LIMIT 1 FOR UPDATE")
            cursor.execute(query)
            if cursor.execute is 0:
                return None
            row = cursor.fetchone()
            update_query=("UPDATE urls SET status='downloading ' WHERE 'index'=%d")%(row['index'])
            cursor.execute(update_query)
            con.commit()
            return row

        except mysql.connector.Error as err:
            # print 'dequeueUrl() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()

    def finishUrl(self,index):
        con = self.cncpool.get_connection()
        cursor= con.cursor()
        try:
            #dont neet to updat done_time using time.strftime ('%Y-%m-%d %H:%M:%S') as it's auto updated
            update_query=("UPDATE urls SET `status`='done' WHERE `index`=%d") % (index)
            cursor.execute(update_query)
            con.commit()
        except mysql.connector.Error as err:
            # print 'finishUrl() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()

