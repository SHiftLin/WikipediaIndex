#!/usr/bin/python
#-*- coding:utf8 -*-
import os
import sys
import json
import time
import random
import mysql.connector

reload(sys)
sys.setdefaultencoding('utf-8')

class PDBC:
    config={
        "user":"root",
        "password":os.environ["DatabasePassword"],
        "database":"WikiIndex"
    }
    conn=None

    def __init__(self):
        self.conn=mysql.connector.connect(**self.config)

    def __del__(self):
        if self.conn!=None:
            conn.commit()
            self.conn.close()

    def insertIndex(self,datalist):
        n=len(data)
        if n==0:
            return
        values=self.getFromat(n)
        data=self.getTuple(datalist)
        cur=self.conn.cursor()
        cur.execute("INSERT INTO wikiindex(word,start,length,count) VALUES "+values,data)
        cur.close()
        

# Make sure data is committed to the database
cnx.commit()

pdbc=PDBC()

if len(sys.argv)<3:
    print "need input file"
    exit();

i=0; mod=100; data=[]
with open(sys.argv[2],"r") as fin:
    [word,triple]=fin.readline().split()
    [start,length,count]=triple.split(',')
    data.append((word,int(start),int(length),int(count)))
    if i%100==0:
        pdbc.insertIndex(data)
        data=[]

pdbc.insertIndex(data)



