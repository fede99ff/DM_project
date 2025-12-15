import preprocess as pre
import loading as ld
import query as qy
import memory_stat as ms

import pandas as pd
import mysql.connector

from pymongo import MongoClient

if __name__ == "__main__":
    #Connect to both databases
    mongo_client = MongoClient("mongodb://dm_proj:password@localhost:27017/?authSource=admin")
    mysql_client = mysql.connector.connect(
                                            host="127.0.0.1",
                                            port=3306,
                                            user="user",
                                            password="password",
                                            database="movies_db"
                                           )
    
    #Used to skip preprocessing and loading if already done
    request = int(input("Do you want to preprocess and load the dataset before querying? (1 for YES, 0 for NO) "))
    
    if request == 1:
        print("Starting preprocessing...")
        dataset = pre.preprocess_dataset()
        print("Preprocessing completed.")
        
        print("----------------------------STARTING TO LOAD DATASET ON MONGODB----------------------------")
        ld.load_data_to_mongoDB(dataset[7], dataset[8], mongo_client)
        print("----------------------------DATA LOADED ON MONGODB----------------------------")

        print("----------------------------STARTING TO LOAD DATASET ON MYSQL----------------------------")
        ld.load_data_to_MYSQL(dataset[0], dataset[1], dataset[2], dataset[3], dataset[4], dataset[5], dataset[6], mysql_client)
        print("----------------------------DATA LOADED ON MYSQL----------------------------")
        
        df_loadingtime = pd.read_csv("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\preprocess_time.csv", low_memory=False) 
        print(df_loadingtime.groupby(df_loadingtime['Database'])['time'].mean())

    qy.query(mongo_client, mysql_client)
    
    #Done here as the index creation happens inside the query function
    ms.mongo_stat(mongo_client)
    ms.mysql_stat(mysql_client)



    
