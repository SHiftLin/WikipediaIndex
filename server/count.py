#!/usr/bin/python
#-*- coding:utf8 -*-
import os
import sys

if len(sys.argv)<2:
    print "need input file"
    exit();

cnt=0
maxl=0
with open(sys.argv[1],"r") as fin:
    for line in fin:
        [word,triple]=line.split()
        cnt+=1
        maxl=max(maxl,len(word))

print cnt
print maxl