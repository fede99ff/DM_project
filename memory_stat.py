import csv

#Get memory storage statistics regarding collection/table sizes in both databases
def mongo_stat(mongo_client):
    with open("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\memory_stat.csv", 'a', newline='', encoding='utf-8') as file_csv:
        
        writer = csv.writer(file_csv)
        db = mongo_client["movies_mongo_db"]

        stats_m = db.command("collStats", "movies")
        stats_p = db.command("collStats", "people")

        #Everything is in bytes
        data_size_m = stats_m["size"]                 
        storage_size_m = stats_m["storageSize"]        #(allocated on disk)
        index_size_m = stats_m["totalIndexSize"]      
        total_size_m = stats_m["totalSize"]            #(data + indexes)

        data_size_p = stats_p["size"]                  
        storage_size_p = stats_p["storageSize"]       #(allocated on disk)    
        index_size_p = stats_p["totalIndexSize"]      
        total_size_p = stats_p["totalSize"]           #(data + indexes)

        #Convert bytes to megabytes (MB) by dividing by 1024^2
        writer.writerow(["Mongo Data size MB:", " MOVIES:",data_size_m / 1024**2, " PEOPLE:", data_size_p / 1024**2])
        writer.writerow(["Mongo Storage size MB:", " MOVIES:",storage_size_m / 1024**2, " PEOPLE:", storage_size_p / 1024**2])
        writer.writerow(["Mongo Index size MB:", " MOVIES:",index_size_m / 1024**2, " PEOPLE:", index_size_p / 1024**2])
        writer.writerow(["Mongo Total size MB:", " MOVIES:",total_size_m / 1024**2, " PEOPLE:", total_size_p / 1024**2])

def mysql_stat(mysql_client):
    with open("C:\\Users\\feder\\OneDrive\\Desktop\\DM_project\\results\\memory_stat.csv", 'a', newline='', encoding='utf-8') as file_csv: 
        writer = csv.writer(file_csv)
        
        cursor = mysql_client.cursor(dictionary=True)
        #Query to get memory statistics for all tables in the movies_db
        cursor.execute("""
            SELECT 
                table_name AS name,
                data_length AS data_bytes,
                index_length AS index_bytes,
                (data_length + index_length) AS total_bytes
            FROM information_schema.TABLES
            WHERE table_schema = %s;
        """, ("movies_db",))
        #Retrieve all results of the query
        rows = cursor.fetchall()

        for row in rows:
            writer.writerow(["MySQL Table",
                row["name"],
                row["data_bytes"] / 1024**2,
                row["index_bytes"] / 1024**2,
                row["total_bytes"] / 1024**2
            ])
