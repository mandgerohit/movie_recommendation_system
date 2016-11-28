import os
#import codecs
import pyspark
from pyspark import SparkConf, SparkContext

APP_NAME = "Create Datasets"

def main(sc):
    super_dir = '/home/rnmandge/'
    os.chdir(super_dir)
    
    movies = []
    with open("movie_data.csv", "r") as lines:
        for line in lines:
            movies.append(line.strip())
    
    data_size = len(movies)
    print data_size
    
    small_size = data_size/5
    small_medium_size = 2*data_size/5
    medium_size = 3*data_size/5
    large_size = 4*data_size/5
    
    small_data = movies[0:small_size]
    print len(small_data)
    
    small_medium_data = movies[0:small_medium_size]
    print len(small_medium_data)
    
    medium_data = movies[0:medium_size]
    print len(medium_data)
    
    large_data = movies[0:large_size]
    print len(large_data)
    
    print "small data"
    fd = open('small_movie_data.csv','a')
    for item in small_data:
      fd.write(item + "\n")
    fd.close()
    
    print "small medium data"
    fd = open('small_medium_movie_data.csv','a')
    for item in small_medium_data:
      fd.write("%s\n" % item)
    fd.close()
    
    print "medium data"
    fd = open('medium_movie_data.csv','a')
    for item in medium_data:
      fd.write("%s\n" % item)
    fd.close()
    
    print "large data"
    fd = open('large_movie_data.csv','a')
    for item in large_data:
      fd.write("%s\n" % item)
    fd.close()

if __name__ == "__main__":
    #Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://152.46.19.250:7077").set("spark.executor.cores","8").set("spark.submit.deployMode","cluster").set("spark.executor.memory","2g")
    sc   = SparkContext(conf=conf)
    #Execute Main functionality
    main(sc)