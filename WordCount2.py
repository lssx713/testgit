# -*- coding: utf-8 -*-
"""
Created on Fri Jul 12 11:30:15 2019

@author: zhz
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

#初始化SparkSession程序入口
spark = SparkSession.builder.appName("WordCount").getOrCreate()
#读入文档
ds_lines = spark.read.text(r"D:\WordCount\WordCount.txt")
#针对df特定的计算格式
words = ds_lines.select(
       explode(
           split(ds_lines.value, " ")
       ).alias("word")
    )
#返回的RDD进行计数
wordCounts = words.groupBy("word").count()
#展示
wordCounts.show()
#关闭spark
spark.stop()