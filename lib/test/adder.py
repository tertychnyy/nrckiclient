def getjobs():
    pass

if __name__ == '__main__':
    conn = MySQLdb.connect(host=self.dbhost, db=self.dbname,
                                        port=self.dbport, connect_timeout=self.dbtimeout,
                                        user=self.dbuser, passwd=self.dbpasswd)
    cur = conn.cursor()