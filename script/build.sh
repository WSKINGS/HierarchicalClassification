#!/usr/bin/env bash

git pull origin master
mvn clean package -Dmaven.test.skip=true