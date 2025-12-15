import csv
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
from kagglehub import KaggleDatasetAdapter

#Preprocessing function specific for MYSQL database
def MYSQL_preprocess(df_movie, df_rating, df_actor, df_crew, people_list):

    movie_table = df_movie.copy()

    rating_table = df_rating.copy()

    #Defining the table for the genres and the one many to many relation between movies and genres
    movies_genres = df_movie[['id', 'genres']].copy()

    #This piece of code extracts the information from a JSON string representing a list of dictionaries.
    #The same procedure will be used to extract every other information stored as JSON strings in the dataset
    
    #For each row convert string in the 'genres' column to list of dictionaries
    movies_genres['genres'] = movies_genres['genres'].apply(ast.literal_eval)

    #Explode list to rows: for each element in the list a new row is created with the same values in the other columns
    movies_genres = movies_genres.explode('genres').dropna()

    #Extract genre id and name from the dictionary in the 'genres' column generating two new columns
    movies_genres['genre_id'] = movies_genres['genres'].apply(lambda x: x['id'])
    movies_genres['genre_name'] = movies_genres['genres'].apply(lambda x: x['name'])
    
    #Table for genres with id and name
    movies_genres_table = movies_genres[['id', 'genre_id']].drop_duplicates().copy()
    movies_genres_table = movies_genres_table.rename(columns={'id': 'movie_id'})
    movies_genres_table = movies_genres_table.dropna()
    
    #Table connecting movies to their genres (many-to-many relationship)
    movies_genres = movies_genres.drop(columns=['genres', 'id'])
    movies_genres = movies_genres.drop_duplicates()
    movies_genres = movies_genres.dropna()

    actor_table = df_actor.copy().drop(columns=['person_name', 'gender'])
    crew_table = df_crew.copy().drop(columns=['person_name', 'gender'])    

    movie_table =  movie_table.drop(columns=['genres'])
    return movie_table, movies_genres_table, movies_genres, rating_table, actor_table, crew_table, people_list

#Preprocessing function specific for MONGODB 
def MONGO_preprocess(df_movie, df_rating, df_actor, df_crew, people_list):
    
    #Preparing the mongodb film and person documentes starting from the data that will be embedded in the 
    #movie collection: actor, crew, ratings.
    #Data are first put in the right format (list of dictionaries) and then grouped by movie id to have
    #all the information (all the actors/crew members/ratings)for a movie in a single document.
    
    actors = df_actor[['movie_id', 'characters', 'person_id']].copy()
    #Create a dictionary for each row with character and person_id 
    actors['characters'] = actors[['characters', 'person_id']].to_dict(orient='records')
    #Group by movie_id to have a list of dictionaries with characters for each movie
    actors_group = actors.groupby('movie_id').agg(
        actor=('characters', list),
    ).reset_index()

    crews = df_crew[['movie_id', 'person_id', 'job', 'department']].copy()
    crews['crew'] = crews[['person_id', 'job', 'department']].to_dict(orient='records') 
    crews_group = crews.groupby('movie_id').agg(
        crew=('crew', list)
    ).reset_index()

    ratings = df_rating. copy()
    ratings['rating'] = ratings[['user_id', 'rating']].to_dict(orient='records')
    ratings = ratings.groupby('movie_id').agg(
        rating=('rating', list)
    ).reset_index()

    #Joining all the information to form the movies collection
    movies_for_mongo = df_movie.copy()
    movies_for_mongo = movies_for_mongo.merge(
        actors_group, left_on='id', right_on='movie_id', how='left'
        ).drop(columns='movie_id').merge(
            crews_group, left_on='id', right_on='movie_id', how='left'
        ).drop(columns='movie_id').merge(
            ratings, left_on='id', right_on='movie_id', how='left'
        ).drop(columns='movie_id')
    
    #Ensure that missing lists are represented as empty lists and not as strings (would cause issues in queries)
    movies_for_mongo['rating'] = movies_for_mongo['rating'].apply(lambda x: x if isinstance(x, list) else [])

    #Preparing the documents for mongodb people collection
    acting_roles = df_actor[['movie_id', 'characters', 'person_id']].copy()
    acting_roles['characters'] = acting_roles[['characters', 'movie_id']].to_dict(orient='records')

    acting_roles = acting_roles.groupby('person_id').agg(
        role = ('characters', list)
    ).reset_index()

    #Creating job list covered by each person in different movies
    crew_roles = df_crew[['movie_id', 'job', 'department', 'person_id']].copy()
    crew_roles['job'] = crew_roles[['movie_id', 'job', 'department']].to_dict(orient='records')
    
    crew_roles = crew_roles.groupby('person_id').agg(
        job = ('job', list)
    ).reset_index()
    
    people_list_for_mongo = people_list.copy()
    people_list_for_mongo = people_list_for_mongo.merge(
        acting_roles, left_on='person_id', right_on='person_id', how='left'
    ).merge(
        crew_roles, left_on='person_id', right_on='person_id', how='left'
    )
    #people_list_for_mongo = people_list_for_mongo.drop(columns=['_id'])
    people_list_for_mongo['role'] = people_list_for_mongo['role'].apply(lambda x: x if isinstance(x, list) else [])
    people_list_for_mongo['job'] = people_list_for_mongo['job'].apply(lambda x: x if isinstance(x, list) else [])

    return movies_for_mongo, people_list_for_mongo


#Common part of the preprocessing
def preprocess_dataset():
    #Datasets from Kaggle. These datasets are part of "The Movies Dataset" available at
    #https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset. 

    file_path = "C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\dataset\\movies_metadata.csv"
    df_movie = pd.read_csv(file_path, low_memory=False)

    file_path_ratings = "C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\dataset\\ratings.csv"
    df_rating = pd.read_csv(file_path_ratings)

    file_path_credits = "C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\dataset\\credits.csv"
    df_crew = pd.read_csv(file_path_credits)

    #Defining the movies table/info useful for the collection
    df_movies = df_movie[['id', 'budget', 'title', 'original_title',
                             'original_language', 'overview', 'popularity',
                             'release_date', 'revenue', 'runtime', 'status', 'genres']].drop_duplicates().copy()
    df_movies = df_movies.dropna()
    df_movies['id'] = pd.to_numeric(df_movies['id'], errors='coerce')
    df_movies['id'] = df_movies['id'].astype(int)
    df_movies = df_movies.drop_duplicates(subset=['id'], keep='first')


    df_movies['budget'] = pd.to_numeric(df_movies['budget'], errors='coerce').fillna(0).astype(int)
    df_movies['revenue'] = pd.to_numeric(df_movies['revenue'], errors='coerce').fillna(0).astype(int)
    df_movies['runtime'] = pd.to_numeric(df_movies['runtime'], errors='coerce').fillna(0).astype(float)
    df_movies['popularity'] = pd.to_numeric(df_movies['popularity'], errors='coerce').fillna(0).astype(float)
    
    movies_table = df_movies.copy()
    #Definng the table of the ratings
    df_rating = df_rating.drop(columns=['timestamp'])
    df_rating = df_rating.drop_duplicates()
    df_rating = df_rating.dropna()
    
    #Rename columns to have same name of the relational database schema
    df_rating = df_rating.rename(columns={'userId': 'user_id', 'movieId': 'movie_id'})
    #Getting reed of ratings for movies not present in movies_table
    df_rating = pd.merge(df_rating, movies_table, left_on='movie_id', right_on='id', how='inner')
    df_rating = df_rating[['user_id', 'movie_id', 'rating']]
    df_rating = df_rating.drop_duplicates(subset=["user_id", "movie_id"], keep='first')

    #Preparing the actors and crew members tables from the credits dataframe. Data have to be
    #converted from JSON strings to Python objects to extract meaningful information for both mongodb
    #and mysql for this is included in the common part.

    #Convert string list to list of dictionaries
    df_actors = df_crew[['id','cast']].copy()
    df_actors['cast'] = df_actors['cast'].apply(ast.literal_eval)

    #Explode cast list to rows
    df_actors = df_actors.explode('cast').dropna()

    #Extract information from the dictionary columns
    df_actors['person_id'] = df_actors['cast'].apply(lambda d: d['id'])
    df_actors['person_name'] = df_actors['cast'].apply(lambda d: d['name'])
    df_actors['gender'] = df_actors['cast'].apply(lambda d: d['gender'])
    df_actors['character'] = df_actors['cast'].apply(lambda d: d['character'])

    #Keep only final needed columns for many to many relationship with movies
    df_actors = df_actors[['id','character', 'person_id', 'person_name', 'gender']].drop_duplicates()
    df_actors = df_actors.rename(columns={'id': 'movie_id'})

    #Split and explode character list to rows. Some actors who played multiple characters in a movie
    #have their characters listed with a single string separated by special characters such as '/', ',', ';', '|'
    df_actors['character'] = df_actors['character'].str.split(r'[\\/,;|]')
    df_actors = df_actors.explode('character').reset_index(drop=True)
    df_actors['character'] = df_actors['character'].str.strip()
    df_actors = df_actors.rename(columns={'character': 'characters'})

    #List of unique actors. Actors_list will be used to produce the people table in the relational database, while
    #df_actors is used to produce the table of acting roles in films (many-to-many relationship). Similar work will 
    #be done for crew members. Results will be used also for mongodb people collection.

    actors_list = df_actors[['person_id','person_name', 'gender']].drop_duplicates().copy()

    crew = df_crew[['id','crew']].copy()
    crew['crew'] = crew['crew'].apply(ast.literal_eval)

    crew = crew.explode('crew').dropna()

    # Normalize the dictionary columns
    crew['person_id'] = crew['crew'].apply(lambda x: x['id'])
    crew['person_name'] = crew['crew'].apply(lambda x: x['name'])
    crew['gender'] = crew['crew'].apply(lambda x: x['gender'])
    crew['job'] = crew['crew'].apply(lambda x: x['job'])
    crew['department'] = crew['crew'].apply(lambda x: x['department'])

    # Keep only final needed columns for crew role in a film. 
    crew = crew[['id','job','person_id', 'person_name', 'gender', 'department']].drop_duplicates()
    crew = crew.rename(columns={'id': 'movie_id'})

    # List of unique crew members
    crew_list = crew[['person_id','person_name', 'gender']].drop_duplicates().copy()

    df_actors = df_actors[df_actors['movie_id'].isin(movies_table['id'])].drop_duplicates()
    df_actors = df_actors.dropna()
    
    crew = crew[crew['movie_id'].isin(movies_table['id'])].drop_duplicates()
    crew = crew.dropna()
    
    #Creating the people list merging actors and crew members
    people_list = pd.concat([actors_list, crew_list])
    people_list = people_list.drop_duplicates()
    people_list = people_list.drop_duplicates(subset=['person_id'], keep='first')
    people_list = people_list.dropna()
    
    #Measuring preprocessing time for both databases only in the independent parts
    with open("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\preprocess_time.csv", 'a', newline='', encoding='utf-8') as file_csv:
        
        writer = csv.writer(file_csv)
        starting_time = time.perf_counter()
        mysql_tables = MYSQL_preprocess(df_movies, df_rating, df_actors, crew, people_list)
        ending_time = time.perf_counter()
        report_partial = ending_time - starting_time
        writer.writerow(['MySQL', report_partial])

        starting_time = time.perf_counter()
        mongo_tables = MONGO_preprocess(df_movies, df_rating, df_actors, crew, people_list)
        ending_time = time.perf_counter()
        report_partial = ending_time - starting_time
        writer.writerow(['MongoDB', report_partial])

    return mysql_tables + mongo_tables