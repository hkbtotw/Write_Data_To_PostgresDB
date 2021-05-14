# -*- coding: UTF-8 -*-
import psycopg2
import datetime
import re

class PostgreSQLOperator:

    def __init__(self, db_host, db_name, db_username, db_password):
        self.PGSQL = psycopg2.connect(host=db_host, database=db_name, user=db_username, password=db_password)

        self.cursorPGSQL = self.PGSQL.cursor()
        self.database_name = db_name

    def close(self):
        self.cursorPGSQL.close()
        del self.cursorPGSQL

    def dropDatabase(self, db_name):
        dropDB = "DROP DATABASE IF EXISTS %s" % (db_name)
        self.cursorPGSQL.execute(dropDB)
        self.PGSQL.commit()

    def dropTable(self, tb_name):
        dropTable = "DROP TABLE IF EXISTS `%s`.`%s`" % (tb_name)
        self.cursorPGSQL.execute(dropTable)
        self.PGSQL.commit()

    def createDB(self, db_name):
        createDB = "CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" % (db_name)
        self.cursorPGSQL.execute(createDB)
        self.PGSQL.commit()

    def createTable(self, tb_name, dataField):
        fieldList = []
        fieldList.append("`%s` %s" % ("id", "INT NOT NULL AUTO_INCREMENT"))
        for field, datatype in dataField.items():
            fieldList.append("`%s` %s" % (field, datatype))
        createTable = "CREATE TABLE IF NOT EXISTS `%s`.`%s` ( %s , PRIMARY KEY (id)) CHARSET=utf8mb4;" % (
        self.database_name, tb_name, ",".join(fieldList))
        self.cursorPGSQL.execute(createTable)
        self.PGSQL.commit()

    def insertDatabase(self, tb_name, dict_insert):
        query = "INSERT INTO %s ( " % (tb_name)
        arge = "VALUES ( "
        for k, v in dict_insert.items():
            query = query + "%s, " % (k.replace("\ufeff", ""))
            if type(v) is datetime.datetime:
                arge += "'%s', " % (v.strftime('%Y-%m-%d %H:%M:%S'))
            elif type(v) is datetime.date:
                arge += "'%s', " % (v.strftime('%Y-%m-%d'))
            elif v == None or v == "NULL" or v == "":
                arge += "NULL, "
            elif type(v) is str:
                arge += "'%s', " % (re.sub('\s|\'|\"|\\\\', ' ', "%s" % (v)).replace("'",""))
            else:
                arge += "'%s', " % (v)
        query = query[:-2] + ') '
        arge = arge[:-2] + ') '
        print(query+arge)
        if self.cursorPGSQL.execute(query + arge) == 0:
            return 0
        self.PGSQL.commit()
        return 1

    def insertDatabaseList(self, tb_name, list_insert):
        query = "INSERT INTO %s ( " % (tb_name)
        arge = "VALUES "
        count = 0
        for dict_insert in list_insert:
            arge += "( "
            for k, v in dict_insert.items():
                if count == 0:
                    query += "%s, " % (k.replace("\ufeff", ""))
                if type(v) is datetime.datetime:
                    arge += "'%s', " % (v.strftime('%Y-%m-%d %H:%M:%S'))
                elif type(v) is datetime.date:
                    arge += "'%s', " % (v.strftime('%Y-%m-%d'))
                elif v == None or v == "NULL" or v == "":
                    arge += "NULL, "
                elif type(v) is str:
                    arge += "'%s', " % (re.sub('\s|\'|\"|\\\\', ' ', "%s" % (v)).replace("'", ""))
                else:
                    arge += "'%s', " % (v)
            arge = arge[:-2] + '), '
            count += 1
        query = query[:-2] + ') '
        arge = arge[:-2]
        #print(query+arge)
        if self.cursorPGSQL.execute(query + arge) == 0:
            return 0
        self.PGSQL.commit()
        return 1

    def executeDatabase(self, query):
        if self.cursorPGSQL.execute(query) == 0:
            return 0
        self.PGSQL.commit()
        return 1

    def selectDatabase(self, query):
        table = []
        if self.cursorPGSQL.execute(query) == 0:
            return table
        columns = [column[0] for column in self.cursorPGSQL.description]
        row = self.cursorPGSQL.fetchone()
        while row:
            temp_dict = {}
            for i in range(0, len(columns)):
                k = columns[i]
                tmp = row[i]
                if tmp == None:
                    tmp = ""
                elif isinstance(tmp, datetime.datetime):
                    tmp = tmp
                    # tmp = tmp.strftime('%Y-%m-%d %H:%M:%S')
                elif isinstance(tmp, datetime.date):
                    tmp = tmp
                    # tmp = tmp.strftime('%Y-%m-%d')
                elif type(tmp) == str:
                    tmp = re.sub('\s', ' ', str(tmp))
                else:
                    pass
                temp_dict[k] = tmp
            table.append(temp_dict)
            row = self.cursorPGSQL.fetchone()
        return table

    def getCursorPGSQL(self):
        return self.cursorPGSQL

    def getSSSQL(self):
        return self.PGSQL

