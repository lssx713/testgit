# -*- coding: utf-8 -*-
"""
Created on Thu Jul 11 10:53:46 2019

@author: zhz
"""

from pyspark.context import SparkContext

import jieba

sc = SparkContext("local", "WordCount")#初始化配置

data = sc.textFile(r"D:\WordCount.txt")#读取是utf-8编码的文件
with open(r'd:\中文停用词库.txt','r',encoding='utf-8')as f:
    x=f.readlines()
    #print (x)
    stop=[i.replace('\n','') for i in x]
    print(stop)

stop.extend(['，','的','我','他','','。',' ','\n','？','；','：','-','（','）','！','1909','1920','325','B612','II','III','IV','V','VI','—','‘','’','“','”','…','、'])#停用标点之类

data=data.flatMap(lambda line: jieba.cut(line,cut_all=False)).filter(lambda w: w not in stop).map(lambda w:(w,1)).reduceByKey(lambda w0,w1:w0+w1).sortBy(lambda x:x[1],ascending=False)

print(data.take(10))