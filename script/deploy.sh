#!/usr/bin/env bash

sparkHome=/home/spark/spark-1.5.2-bin-hadoop2.6
jarPath=/home/wangshuai/workspace/HierarchicalClassification/target/hierarchicalClassification-jar-with-dependencies.jar
sparkHost=spark://10.1.0.149:7077
featureClass=com.ws.application.GenerateSpace
trainClass=com.ws.application.TrainModel
classifyClass=com.ws.application.Classify

#$sparkHome/bin/spark-submit --class com.ws.application.Classify --executor-memory 6G $jarPath $sparkHost

if [ $# -gt 0 ];
then
  mainClass=$1 +"Class"
  $sparkHome/bin/spark-submit --class $mainClass --executor-memory 6G $jarPath $sparkHost
else
    echo "deploy.sh type[feature; train; classify]"
fi
