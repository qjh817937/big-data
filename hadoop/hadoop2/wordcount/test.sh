#!/bin/bash

export PATH=$PATH:/home/a/share/hadoop-2.3.0-cdh5.1.0/bin/

echo "word1 word2 word3" > data.txt
hadoop dfs -mkdir  /user/jianshen/input
hadoop dfs -copyFromLocal ./data.txt /user/jianshen/input/data.txt
hadoop dfs -rm -r /user/jianshen/output

hadoop jar target/hadoop2test-1.0-SNAPSHOT-jar-with-dependencies.jar HelloWorld  /user/jianshen/input /user/jianshen/output      

hadoop dfs -cat /user/jianshen/output/*
