import pandas as pd
import json
import pymongo
import pymongoarrow
import mysql.connector
import time
import csv
import psutil
import docker

from sqlalchemy import create_engine, null
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.exc import SQLAlchemyError
from pymongo import MongoClient

#Define the queries for both databases
mongo_queries = {
    'Q1': [
        {'$match': {'id': 862}},
        {'$project': {'title': 1, 'budget': 1, 'revenue': 1, 'release_date': 1, 'original_language': 1}}
        ],
    
    'Q2':[
        {'$match': {'popularity': {'$gte': 90}}},
        {'$project': {'popularity':1, 'title':1, 'release_date':1}}
        ],
    
    'Q3':[
        {'$match': {'revenue': {'$lt': 5000000, '$gt': 0}, 'budget': {'$gt': 5000000}}},
        {'$project': {'title': 1}}
        ],
    
    'Q4':[
        {'$match': {'role': {'$ne': []}}},
        {'$project': {'person_id': 1, 'role_size': {'$size':'$role'}}},
        {'$match': {'role_size': {'$gt': 100}}}
        ],
    
    'Q5':[
        {'$match': {'gender': 1}},
        {'$unwind': '$job'},
        {'$match': {'job.job': 'Writer'}},
        {'$project': {'name': 1, 'crew.movie_id': 1}}
        ],
    
    'Q6':[
        { "$match": { "rating": { "$ne": []} }},
        {'$unwind': '$rating'},
        {'$group': {'_id': '$id',
                    'title': {'$first': '$title'},
                    'average_rating': {'$avg': '$rating.rating'},
                    'number_of_rating': {'$sum': 1 }}
                    },
        {'$match': {'average_rating': {'$gt': 3.5}}},
        {'$match': {'number_of_rating': {'$gte': 1000}}},
        {'$project': {'_id': 0}}
        ],
    
    'Q7':[ 
        {'$sort': { 'revenue': -1 } },
        {'$limit': 50 },
        {'$unwind': '$actor'},
        {'$group': {'_id': '$actor.person_id' }},
        {'$lookup':  {
             'from': 'people',
             'localField': '_id',
             'foreignField': 'person_id',
             'as': 'person_info'
           }},
        {'$unwind': '$person_info'},
        {'$project': {'person_info.person_name': 1, '_id': 0}},  
        ],
    
    'Q8':[
        { "$match": { "rating": { "$ne": []} }},
        {'$unwind': '$rating'},
        {'$match': {'rating.rating': {'$eq': 5}}},
        {'$unwind': '$actor'},
    
        {'$group': {'_id': '$actor.person_id' } },
        {'$lookup': {'from': 'people',
                 'localField': '_id',
                 'foreignField': 'person_id',
                 'as': 'person_info'}},
        {'$unwind': '$person_info'},
        {'$project': {'person_name': '$person_info.person_name'}}   
        ],
    
    'Q9':[  
        { '$unwind': '$crew' },
        { '$match': { 'crew.job': 'Director' } },
        { '$unwind': '$rating' },
        { '$group': {
            '_id': '$crew.person_id',
            'avg_revenue': { '$avg': '$revenue' },
            'avg_rating': { '$avg': '$rating.rating' },
            'rating_count': { '$sum': 1 }
          }
        },
        { '$match': { 'rating_count': { '$gte': 100 } } },
        { '$sort': { 'avg_revenue': -1 } },
        ],
    
    'Q10':[
        { "$match": { "rating": { "$ne": [] } }},
        { "$project": { "actor": 1, "movie_avg": { "$avg": "$rating.rating" }, "movie_count": { "$size": "$rating" }}},
        { "$unwind": "$actor" },
        { "$group": {
                    "_id": "$actor.person_id",
    
                    "total_weighted_sum": { "$sum": { "$multiply": ["$movie_avg", "$movie_count"] } },
                    "total_count": { "$sum": "$movie_count" }       
            }
        },
    
        { "$project": {"actor_avg": { "$divide": ["$total_weighted_sum", "$total_count"] }}},
        { "$match": {"actor_avg" : { "$gt" : 3.5} } },
        { "$lookup": {
            "from": "people",
            "localField": "_id",
            "foreignField": "person_id",
            "as": "person_info"
          }
        },
        { "$unwind": "$person_info" },
        { "$project": {"person_name": "$person_info.person_name", "actor_avg": 1 ,"_id": 0 }}
        ]
    
}
#In Q10 of mongo you have to calculate the weighted average for each actor using the formula:
#actor_avg = sum(movie_avg * movie_count) / sum(movie_count) so for each movie
#of the actor we multiply the average rating of the movie by the number of ratings received
#and we sum these values to get the total weighted sum, then we divide this by the total number of ratings
#received (i.e., the sum of movie_count for all movies of the actor).

mysql_queries = {
    'Q1': """
        SELECT title, budget, revenue, release_date, original_language 
        FROM movies
        WHERE id = 862;
    """,
    
    'Q2': """
        SELECT popularity, title, release_date
        FROM movies
        WHERE popularity >= 90;
    """,
    
    'Q3': """
        SELECT title 
        FROM movies 
        WHERE revenue < 5000000 AND revenue > 0 AND budget > 5000000;
    """,
    
    'Q4' : """
        SELECT person_id, COUNT(*)
        FROM actors
        GROUP BY person_id
        HAVING COUNT(*) > 100;
        """,
    
    'Q5': """
        SELECT p.person_id
        FROM people p
        JOIN crew c ON p.person_id = c.person_id
        WHERE c.job = 'Writer' AND p.gender = 1;
    """,
    
    'Q6': """ 
        SELECT m.id, m.title, agg.average_rating
        FROM movies m
        JOIN (
            SELECT movie_id, AVG(rating) AS average_rating
            FROM ratings
            GROUP BY movie_id
            HAVING COUNT(*) >= 1000 AND AVG(rating) > 3.5
        ) agg
        ON m.id = agg.movie_id;
    """,
    
    'Q7': """
        SELECT DISTINCT p.person_name
        FROM (
            SELECT id
            FROM movies
            ORDER BY revenue DESC
            LIMIT 50
        ) AS m
        JOIN actors a ON m.id = a.movie_id
        JOIN people p ON a.person_id = p.person_id;
    """,
    
    'Q8': """
        SELECT p.person_name
        FROM actors a 
        JOIN people p ON a.person_id = p.person_id
        JOIN ratings r ON a.movie_id = r.movie_id
        WHERE r.rating = 5
        GROUP BY p.person_id;
    """,
    
    'Q9': """
        SELECT c.person_id, AVG(r.rating) as avg_rating, COUNT(r.rating) as rating_count, AVG(m.revenue) as avg_revenue
        FROM crew c
        JOIN ratings r ON c.movie_id = r.movie_id
        JOIN movies m ON c.movie_id = m.id
        WHERE c.job = 'Director'
        GROUP BY c.person_id
        HAVING COUNT(r.rating) >= 100
        ORDER BY AVG(r.rating) DESC;
    """,
    
    'Q10': """
        SELECT p.person_name
        FROM ratings r
        JOIN actors a ON r.movie_id = a.movie_id
        JOIN people p ON p.person_id = a.person_id
        GROUP BY a.person_id
        HAVING AVG(r.rating) > 3.5;
    """
}

#Define indexes for MySQL here to simplify their creation
mysql_index = {
    'idx_movie_id_ratings': """
        CREATE INDEX idx_movie_id_ratings ON ratings(movie_id);
    """,
    
    'idx_ratings_movieid_rating': """
        CREATE INDEX idx_ratings_movieid_rating ON ratings(movie_id, rating);
    """,

    'idx_actors_movie_id': """
        CREATE INDEX idx_actors_movie_id ON actors(movie_id);
    """,
    
    'idx_person_id_actors': """
        CREATE INDEX idx_person_id_actors ON actors(person_id);
    """,

    'idx_crew_movie_id': """
        CREATE INDEX idx_crew_movie_id ON crew(movie_id);
    """,

    'idx_person_id_crew': """
        CREATE INDEX idx_person_id_crew ON crew(person_id);
    """
}

#Getting memory usage of a docker container to monitor memory consumption of queries
def get_container_mem_mb(container):
    stats = container.stats(stream=False)
    return stats["memory_stats"]["usage"] / (1024**2)  

def create_mysql_indexes(client):
    cursor = client.cursor()
    for index in mysql_index.items():
        try:
            #Execute each index creation statement retrived from the mysql_index dictionary
            cursor.execute(index[1])
        except SQLAlchemyError as e:
            print(f"Error creating index {index[0]}: {e}")
    cursor.close()
    return

def create_mongo_indexes(client):
    db = client["movies_mongo_db"]
    #Get collections to create indexes on each of them
    movies_collection = db["movies"]
    people_collection = db["people"]
    
    #Indexes are directly defined here for mongoDB
    try:
        movies_collection.create_index([('id', pymongo.ASCENDING)], name='idx_movie_id')
        movies_collection.create_index([('rating.rating', pymongo.ASCENDING)], name='idx_movie_rating')
        people_collection.create_index([('person_id', pymongo.ASCENDING)], name='idx_person_id')
    except Exception as e:
        print(f"Error creating MongoDB indexes: {e}")
    return

def mongo_query(client):

    with open("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\queries_time.csv", 'a', newline='', encoding='utf-8') as file_csv:
        writer = csv.writer(file_csv)
        db = client["movies_mongo_db"]
        movies_collection = db["movies"]
        people_collection = db["people"]

        #Get mongo docker container, need to monitor its memory usage
        container = docker.from_env().containers.get("mongo_container")

        for q in mongo_queries.items():
            report_partial = []
            mem_partial = []
            print(f"Executing query {q[0]}...")

            #Measure time and memory for each of the 10 runs
            for i in range(10):
                starting_time = time.perf_counter() 
                #if used to distinguish queries that have to be run on people 
                #collection from those that have to be run on movies collection
                if q[0] == 'Q4' or q[0] == 'Q5':  
                    #aggregate() used to perform queries
                    result = people_collection.aggregate(q[1])
                    #n = 0
                    #for doc in result:
                    #    n += 1
                    
                else:  
                    result = movies_collection.aggregate(q[1])
                    #n = 0
                    #for doc in result:
                    #    n += 1

                #print(f"Number of documents retrieved: {n}")
                ending_time = time.perf_counter()
                mem = get_container_mem_mb(container)
                mem_partial.append(max(mem, 0))

                report_partial.append(ending_time - starting_time)

            writer.writerow(['MongoDB', q[0], sum(report_partial)/len(report_partial), sum(mem_partial)/len(mem_partial)])

    return

def mysql_query(client):

    with open("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\queries_time.csv", 'a', newline='', encoding='utf-8') as file_csv:
        writer = csv.writer(file_csv)
        client_docker = docker.from_env()
        container = client_docker.containers.get("mysql_container")

        for q in mysql_queries.items():
            report_partial = []
            mem_partial = []

            print(f"Executing query {q[0]}...")

            for i in range(10):
                cursor = client.cursor()
                
                starting_time = time.perf_counter()
                cursor.execute(q[1])

                ending_time = time.perf_counter()
                mem = get_container_mem_mb(container)
                
                cursor.fetchall()
                cursor.close()
                report_partial.append(ending_time - starting_time)
                mem_partial.append(max(mem, 0))

                #print(f"Number of rows retrieved: {len(rows)}")

            writer.writerow(['MySQL', q[0], sum(report_partial)/len(report_partial), sum(mem_partial)/len(mem_partial)])

    return

#Define the main query function that will execute queries on both databases. first without indexes, then with indexes
def query(client_mongo, client_mysql):
    
    print("Starting querying on mongoDB...")
    mongo_query(client_mongo)
    create_mongo_indexes(client_mongo)
    print("Starting querying on mongoDB with indexes...")
    mongo_query(client_mongo)
    
    print("Starting querying on MySQL...")
    mysql_query(client_mysql)
    create_mysql_indexes(client_mysql)
    print("Starting querying on MySQL with indexes...")
    mysql_query(client_mysql)

    return

