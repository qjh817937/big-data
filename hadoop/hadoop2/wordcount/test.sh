#!/bin/bash

export PATH=$PATH:/home/a/share/hadoop-2.3.0-cdh5.1.0/bin/

function helloword() 
{
    echo "word1 word2 word3" > data.txt
    hadoop dfs -mkdir  /user/jianshen/input
    hadoop dfs -copyFromLocal ./data.txt /user/jianshen/input/data.txt
    hadoop dfs -rm -r /user/jianshen/output

    hadoop jar target/hadoop2test-1.0-SNAPSHOT-jar-with-dependencies.jar HelloWorld  /user/jianshen/input /user/jianshen/output      

    hadoop dfs -cat /user/jianshen/output/*
}

function secondarysort() 
{
    echo "id=1,timestamp=100" > data.txt
    echo "id=1,timestamp=99" >> data.txt
    echo "id=1,timestamp=101" >> data.txt
    echo "id=2,timestamp=100" >> data.txt
    echo "id=2,timestamp=99" >> data.txt
    echo "id=2,timestamp=101" >> data.txt
    hadoop dfs -rm -r /user/jianshen/input
    hadoop dfs -mkdir  /user/jianshen/input
    hadoop dfs -copyFromLocal ./data.txt /user/jianshen/input/data.txt
    hadoop dfs -rm -r /user/jianshen/output

    hadoop jar target/hadoop2test-1.0-SNAPSHOT-jar-with-dependencies.jar secondarysort.SecondarySortMain /user/jianshen/input /user/jianshen/output      

    hadoop dfs -cat /user/jianshen/output/*
}

secondarysort
