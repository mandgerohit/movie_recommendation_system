import sys
import os
from os import listdir
from os.path import isfile, join
import csv
import pyspark
from pyspark import SparkConf, SparkContext

APP_NAME = "Pre-processor"
super_dir = "/home/rnmandge/"

def preprocess(filename, sc):
    
    movie = str()
    textRDD = sc.textFile(filename)
    f=open(filename)
    
    movie = f.readline().strip().replace(":","")
    fd = open('movie_data2.csv','a')
    for line in f:
        data = line.strip().split(',')
        user=data[0].strip()
        rating=data[1].strip()
        date=data[2].strip()
        fields=movie + "," + user + "," + rating + "," + date + "\n"

        os.chdir(super_dir)
	
        fd.write(fields) 
    fd.close()

def main(sc):
    mypath = "/home/rnmandge/training_set1/"
    allfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    for file in allfiles:
        filename = mypath + file
        
		preprocess(mypath + file, sc)
		print file

if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://152.46.19.250:7077").set("spark.executor.cores","8").set("spark.submit.deployMode","cluster").set("spark.executor.memory","2g")
    sc   = SparkContext(conf=conf)
    # Execute Main functionality
    main(sc)