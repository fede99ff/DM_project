import pandas as pd
import kagglehub
import ast
import json
import pymongo
import pymongoarrow
import mysql.connector
import time 
import csv 

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.exc import SQLAlchemyError
from pymongo import MongoClient


def load_data_to_mongoDB(movies, people, client):
    #measure loading time
    starting_time = time.time() 

    movies = movies.fillna("") 
    people = people.fillna("")

    movies_dict = movies.to_dict(orient='records')
    people_dict = people.to_dict(orient='records')

    db = client["movies_mongo_db"]
    
    movies_collection = db["movies"]
    movies_collection.insert_many(movies_dict)

    people_collection = db["people"]
    people_collection.insert_many(people_dict)
    
    ending_time = time.time()
    
    with open("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\loading_time.csv", 'a', newline='', encoding='utf-8') as file_csv:
        writer = csv.writer(file_csv)
        writer.writerow(['MongoDB', ending_time - starting_time])
    return
    
def load_data_to_MYSQL(movies_table, movies_genres_table, movies_genres, df_rating, df_actors, crew, people_list, client):
    #SQLAlchemy engine created to use it in the to_sql() from pandas to load the dataset 
    engine = create_engine('mysql+mysqlconnector://user:password@localhost:3306/movies_db')
    starting_time = time.time()

    #Loading dataframes to MYSQL database
    movies_table.to_sql('movies', con = engine, if_exists='append', index=False)
    people_list.to_sql('people', con = engine, if_exists='append', index=False)
    movies_genres.to_sql('genres', con = engine, if_exists='append', index=False)
    movies_genres_table.to_sql('movies_genres', con = engine, if_exists='append', index=False)
    
    #Need to chunk the ratings table as it is too large to be loaded at once
    i = 0
    while i < len(df_rating):
        df_rating.iloc[i:i+2500000].to_sql('ratings', con=engine, if_exists='append', index=False)
        i += 2500000

    df_actors.to_sql('actors', con = engine, if_exists='append', index=False)
    crew.to_sql('crew', con = engine, if_exists='append', index=False)
    
    ending_time = time.time()
    
    with open('C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\loading_time.csv', 'a', newline='', encoding='utf-8') as file_csv:
        writer = csv.writer(file_csv)
        writer.writerow(['MySQL', ending_time - starting_time])
    return