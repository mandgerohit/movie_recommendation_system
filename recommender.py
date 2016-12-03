## Imports
import random
from operator import *
from shutil import *
import itertools
import math
import sys
from os import listdir
from os.path import isfile, join
from pyspark import SparkConf, SparkContext
from operator import add


## Constants
APP_NAME = "Recommender"
##OTHER FUNCTIONS/CLASSES

netflix_list = {}
movie = {}
userid = int(sys.argv[1])
year = int(sys.argv[2])
filename = sys.argv[3]

def calculate_similarity(movie1,movie2):
    eucledian_distance=[]
    for movie in netflix_list[movie1]:
      if movie in netflix_list[movie2]:
        eucledian_distance.append(pow(int(netflix_list[movie1][movie]) - int(netflix_list[movie2][movie]),2))

    sum_eucledian_distance=sum(eucledian_distance)

    if sum_eucledian_distance==0:
        return 0
    else:
        return 1/(1+math.sqrt(sum_eucledian_distance))
    
def calculate_pearson_coefficient(movie1,movie2):
    movie_list=[]
    for movie in netflix_list[movie1]:
      if movie in netflix_list[movie2]:
        movie_list.append(movie)

    movie_count= float(len(movie_list))

#If they have no ratings in common, return 0
    if movie_count==0.0:
        return 0

    sum1=0.0
    sum2=0.0

#Add up all the preferences
    for movie in movie_list:
      sum1= sum1+ netflix_list[movie1][movie]

    for movie in movie_list:
      sum2= sum2+ netflix_list[movie2][movie]


#Sum up the squares
    for movie in movie_list:
      sum1_squares= sum1+ pow(netflix_list[movie1][movie],2)

    for movie in movie_list:
      sum2_squares= sum2+ pow(netflix_list[movie2][movie],2)

    product=0.0

#Sum up the products
    for movie in movie_list:
      product= product + netflix_list[movie1][movie] * netflix_list[movie2][movie]

    numerator=0.0
    denominator=0.0
    numerator= (product - (sum1*sum2)/movie_count)
    denominator=math.sqrt((sum1_squares/movie_count-pow(sum1,2)/movie_count*movie_count)*(sum2_squares/movie_count- pow(sum2,2)/movie_count*movie_count))

    if denominator==0.0:
        return 0
    return numerator/denominator
  
def top_similar_movies(movie,n,function=calculate_pearson_coefficient):
    similarity_scores=[]
    for other_movie in netflix_list:
        if other_movie!=movie:
            similarity_scores.append([function(movie,other_movie),other_movie])

    similarity_scores.sort()
    similarity_scores.reverse()
    
    return similarity_scores[:n]

def fetch_movie_titles(movie_filename):
  with open(movie_filename, 'r') as file:
    for line in file:
      data = line.strip().split(',')
      movie[int(data[0])] = data[2]

def filterYear(data):
  arr = data[3].strip().split('-')
  if int(arr[0]) == year:
    return data

def filterUserMovies(data):
  if int(data[1]) == userid:
    return data

def recommend_movies(usermoviesRDD, n):
  usermovies=usermoviesRDD.collect()
  sim_movies = []
  usermovies_size= len(usermovies)
  for i in range(usermovies_size):
    rec = top_similar_movies(int(usermovies[i][0]), 5)
    for entry in rec:
      sim_movies.append((entry[0]*usermovies[i][2], entry[1]))

  sim_movies.sort()
  sim_movies.reverse()
  for i in range(n):
    print movie[sim_movies[i][1]]

def main(sc,movie_title_file):
   allMovieData=sc.textFile(filename).map(lambda x: x.split(","))
   allMovieData=allMovieData.map(lambda x: (int(x[0]),int(x[1]),int(x[2]),str(x[3])))
   allMovieData=allMovieData.map(filterYear).filter(lambda x: x!=None)

   userMovieData=allMovieData.map(filterUserMovies).filter(lambda x: x!=None)
   
   all_movies_list = allMovieData.collect()

   for data in all_movies_list:
      if data[0] in netflix_list:
          netflix_list[data[0]].update({data[1]:data[2]})
      else:
          netflix_list[data[0]] = {data[1]:data[2]}
   
   fetch_movie_titles(movie_title_file)
   recommend_movies(userMovieData, 5)

if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   conf = conf.setMaster("spark://152.46.19.250:7077").set("spark.executor.cores","8").set("spark.submit.deployMode","cluster").set("spark.executor.memory","2g").set("spark.driver.memory","2g")
   sc   = SparkContext(conf=conf)
   movie_title_file = '/home/rnmandge/movie_titles.txt'

   # Execute Main functionality
   main(sc, movie_title_file)