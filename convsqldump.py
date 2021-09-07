import argparse
import mysql.connector
from mysql.connector import errorcode
from pathlib import Path
import base64

"""
Coversion area
"""

from Crypto.Cipher import AES
import base64

def decrypt_aes(oldvalue):
    cipher = AES.new(encryption_key, AES.MODE_CFB, iv, segment_size=128)
    return cipher.decrypt(encrypted_data)

conversion_list = [
    #['user', 'name', conversion_func]
    ]

def convert_columns(table, column, oldvalue):
    for t,c,f in conversion_list:
        if table == table and column == c:
            return f(oldvalue)
    return oldvalue
"""
end
"""

class DBMySQL:
    def __init__(self, ip, usr, pwd, db):
        """ Init for the MySQL class
        :param ip:
            IP address to the MySQL server
        :param usr:
            Username for the lctdb database
        :param pwd:
            Password for the lctdb database
        :param db:
            Name of the lctdb database
        """
        self.ip = ip
        self.usr = usr
        self.pwd = pwd
        self.datbase = db
        self.tablelist = []

    def set_param(self, ip, usr, pwd, db):
        self.ip = ip
        self.usr = usr
        self.pwd = pwd
        self.datbase = db

    def conn(self):
        """ Connect to the MySQL database
        :return:
            0 If success
            1 If unable to connect
            2 Faulty user of password
            4 Unknown error
        """
        try:
            self.db = mysql.connector.connect(host=self.ip,user=self.usr,password=self.pwd,database=self.datbase)
            self.cursor = self.db.cursor()
            return 0
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                return 1 # Database doesnt exist
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                return 2 # Fault user ow password
            return 4 # Unknown error

    def disconn(self):
        """ Disconnect from the MySQL database
        :return:
            Nothing
        """
        if self.cursor:
            self.cursor.close()
        if self.db:
            self.db.close()


    def db_set(self, sql):
        """ Execute a SQL statement towards the database
        :param sql:
            A string with a SQL statement
            not expecting any thing in return
        :return:
            Nothing
        """
        retvalue = self.conn()
        if retvalue:
            return {'error': retvalue, 'row': 0, 'data': 0}
        try:
            # Execute the SQL command
            self.cursor.execute(sql)
            # Commit your changes in the database
            self.db.commit()
            self.disconn()
            return {'error': 0, 'row':self.cursor.lastrowid, 'data': 0}
        except:
            # Rollback in case there is any error
            print("Exception error: {0}".format(sys.exc_info()[0]))
            self.db.rollback()
            self.disconn()
            return {'error': 5, 'row': 0, 'data': 0}


    def db_get(self, sql):
        """ Execute a SQL statement towards the database
        :param sql:
            A string with a SQL statement
            expecting some kind of result
        :return:
            Returning a tuple
        """
        retvalue = self.conn()
        if retvalue:
            return {'error': retvalue, 'row': 0, 'data': 0}
        try:
            # Execute the SQL command
            self.cursor.execute(sql)
            return {'error': 0, 'row': 0, 'data': self.cursor.fetchall()}
        except:
            # Rollback in case there is any error
            print("Exception error: {0}".format(sys.exc_info()[0]))
            return {'error': 5, 'row': 0, 'data': 0}

    # Document further and maybe change name to db_test
    def test_connection(self):
        """ Test the database connection
        :return:
        """
        res = self.conn()
        if not res == 0:
            return res
        sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \'{}\' ".format(self.datbase)
        restup = self.db_get(sql)
        self.disconn()
        if restup is None:
            return 3 # No database
        return res

    def get_tables(self):
        #valuelist = []
        sql = "SHOW TABLES"
        retvalue = self.db_get(sql)
        if retvalue['error']:
            return {'error':retvalue['error'], 'data': 0}
        if len(retvalue['data']):
            for table in retvalue['data']:
                self.tablelist.append(table[0])
        for table in self.tablelist:
            print("Processing table {}".format(table))
            self.get_table_content(table)

    def get_columns_from_table(self, table):
        valuelist = []
        sql = "SHOW COLUMNS FROM %s" % table
        retvalue = self.db_get(sql)
        if retvalue['error']:
            return {'error':retvalue['error'], 'data': 0}
        if len(retvalue['data']):
            for row in retvalue['data']:
                valuelist.append(row[0])
        return valuelist

    def get_table_content(self, table):
        columns = self.get_columns_from_table(table)
        valuelist = []
        sql = "SELECT * FROM %s" % table
        retvalue = self.db_get(sql)
        if retvalue['error']:
            return {'error':retvalue['error'], 'data': 0}
        if len(retvalue['data']):
            for row in retvalue['data']:
                valuelist.append(row)
        ofile = OutputFile(None, table, columns, valuelist)



class OutputFile:
    def __init__(self, dir, tablename, topline, tbuffer):
        if dir == None:
            self.wdir = Path(__file__).resolve().parents[0]
        else:
            self.wdir = dir
        if not Path(self.wdir).is_dir():
            print("Table name {} not written, directory doesn't exist".format(tablename))
            return
        if Path(self.wdir, tablename + '.csv').is_file():
            print("Table name {} not written, file already exists".format(tablename))
            return
        with Path(self.wdir, tablename + '.csv').open('a') as fp:
            ttopline = ','.join(map(str,topline))
            print('{}'.format(ttopline), file=fp)
            index = 0
            for line in tbuffer:
                list_line = list(line)
                length = len(list_line)
                for i in range(length):
                    list_line[i] = convert_columns(tablename, topline[i], line[i])
                fline = ','.join(map(str,list_line))
                print('{}'.format(fline), file=fp)



if __name__ == "__main__":
    imagelist = []
    privesc_parameter = {}
    parser = argparse.ArgumentParser(description='mysql_dump_c v1.0')
    parser.add_argument('-d', '--database', help='name of the db to dump', required=True)
    parser.add_argument('-u', '--user', help='user with access to the db', required=True)
    parser.add_argument('-p', '--password', help='user with access to the db', required=True)

    args = parser.parse_args()

    #only localhost for now
    host = '127.0.0.1'
    dbmysql = DBMySQL(host, args.user, args.password, args.database)
    print(dbmysql.test_connection())
    dbmysql.get_tables()

    

