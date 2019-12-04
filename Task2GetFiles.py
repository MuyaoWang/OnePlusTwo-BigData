#!/usr/bin/env python
# coding: utf-8

#Put required files to task 

import os
from os import path
import shutil 

if not os.path.exists('task2'):
    os.mkdir('task2')
    
currentDirectory = os.getcwd()
dataDirectory = currentDirectory + "/NYCOpenData"
clusterDirectory = currentDirectory + "/cluster3.txt"
taskDataDirectory = currentDirectory + "/task2"

datalst = os.listdir(dataDirectory)

with open(clusterDirectory, 'r') as file:
    data = file.read().replace('\n', '')
    
data = data[1:-1]

splitDatalst = data.split(',')

dictTask = {}  
for i in range(len(splitDatalst)):
    if i == 0:
        data = splitDatalst[i].split('.')[0][1:] + ".tsv.gz"
    else:
        data = splitDatalst[i].split('.')[0][2:] + ".tsv.gz"
    name = splitDatalst[i].split('.')[1]
    if data in datalst:   
        if data in dictTask:
            dictTask[data].append(name)
        else:
            dictTask[data] = [name]
        srcPath = dataDirectory + "/" + data     
        if path.exists(srcPath):
            dstPath = taskDataDirectory + "/" + data
            shutil.move(srcPath, dstPath)

tasklst = os.listdir(taskDataDirectory)
print("Number of files in task2: ", len(tasklst))


print("Save all infos as a json file.")
import json
jsondict = json.dumps(dictTask)
f = open("task2Dict.json","w")
f.write(jsondict)
f.close()

print("Load info as a dictionary called data.")
with open('task2Dict.json') as f:
	data = json.load(f)






    


