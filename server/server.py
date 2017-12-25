#!/usr/bin/python
#-*- coding:utf8 -*-
import os
import sys
import json
import time
import random
import mysql.connector
from flask import Flask
from flask import render_template
from flask import request

app = Flask(__name__)

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

    def queryIndex(self,word):
        cur=self.conn.cursor()
        cur.execute("SELECT * FROM wikiindex WHERE word='"+word+"' LIMIT 1")
        row=cur.fetchone()
        cur.close()
        return row

    def queryPage(self,id):
        pass

pdbc=PDBC()
f_tf=open("resources/TFCalculate","r")
f_page=open("resources/PageOffLen","r")


@app.route('/search')
def search():
    keyword=request.args.get('keyword')
    row=pdbc.queryIndex(keyword)
    data=[]
    if row!=None:
        (word,start,length,count)=row
        f_tf.seek(start)
        n=min(count,20)
        for i in xrange(0,n,1):
            line=f_tf.readline(length)
            (key,value)=line.split('\t')
            (word,tf_str)=key.split(',')
            (page_id_str,isTitle_str)=value.split(',')
            tf=float(tf_str)
            page_id=int(page_id_str)
            data.append({'page_id':page_id,'tf':tf})
    return render_template('results.html',data=data)



@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run()


if f_tf!=None:
    f_tf.close()
if f_page!=None:
    f_page.close()