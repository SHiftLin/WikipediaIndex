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
            self.conn.commit()
            self.conn.close()

    def insertTFIndex(self,values,data):
        n=len(data)
        if n==0:
            return

        cur=self.conn.cursor()
        cur.execute("INSERT INTO TFIndex(word,start,length,count) VALUES "+values,data)
        cur.close()
        self.conn.commit()

    def insertPageIndex(self,values,data):
        n=len(data)
        if n==0:
            return

        cur=self.conn.cursor()
        cur.execute("INSERT INTO pageIndex(pageid,offset,length) VALUES "+values,data)
        cur.close()
        self.conn.commit()

pdbc=PDBC()

if len(sys.argv)<3:
    print "need input file"
    exit();

TFIndex=sys.argv[1] #InvertedIndex
pageIndex=sys.argv[2] #PageOffLen

# with open(TFIndex,"r") as fin:
#     i=0; mod=100
#     data=[]; values=''
#     for line in fin:
#         [word,triple]=line.split()
#         [start,length,count]=triple.split(',')
#         data.append(word)
#         data.append(int(start))
#         data.append(int(length))
#         data.append(int(count))
#         i+=1
#         if i>1:
#             values+=",(%s, %s, %s, %s)"
#         else:
#             values+="(%s, %s, %s, %s)"
#         if i==mod:
#             pdbc.insertTFIndex(values,tuple(data))
#             data=[]
#             values=''
#             i=0
#     pdbc.insertTFIndex(values,data)


with open(pageIndex,"r") as fin:
    i=0; mod=100
    data=[]; values=''
    for line in fin:
        [pageid,offset,length]=line.split()
        data.append(int(pageid))
        data.append(int(offset))
        data.append(int(length))
        i+=1
        if i>1:
            values+=",(%s, %s, %s)"
        else:
            values+="(%s, %s, %s)"
        if i==mod:
            pdbc.insertPageIndex(values,tuple(data))
            data=[]
            values=''
            i=0
    pdbc.insertPageIndex(values,data)

