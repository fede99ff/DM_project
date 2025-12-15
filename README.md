
# Data Management Project  
Comparison of Relational (MySQL) and Non-Relational (MongoDB) Databases Using the Movies Dataset

## Overview
The goal of this project is to compare the performance of a relational database (**MySQL**) and a non-relational database (**MongoDB**) on a real case application, on a specific dataset.  
The selected dataset is **The Movies Dataset** from Kaggle, which contains metadata for approximately 45,000 movies. The data contains informations about movies, actors, crew, ratings etc. stored 
in csv files. The dataset is an assemblance of different sources so the data inside the csv files are of different type and needs to be treated accordingly. The application scenario is the one
in which a company want to allow users or other companies, to retrive information about movies as: "Which are the highest rated movies", "Who are the best directors" or for a company "Which are the genres
with the highest average revenues". To obtained a

The data is preprocessed and reorganized according to:
- an **ER schema** for MySQL  
- a **document-based schema** for MongoDB
and then loaded on the respective DB.


For the comparison instead the matric to evaluate the performance of the DBs on this dataset are:
- data loading time
- query execution time (based on a set of commonly requested movie-related queries.)
- memory usage  

---

## Dataset
The original Kaggle dataset contains several CSV files.  
This project uses:

- `movies.csv` — general metadata about each movie  
- `credits.csv` — information about actors and crew members  
- `ratings.csv` — user ratings for the movies  

These files are cleaned and transformed before being loaded into the databases.
- Notice that by getting the dataset from kaggle, it may happen that in the loading process some entry may cause some problems with foreign key. To solve this one can open the csv file and remove the entry with the id that cause the conflict.
---

## Dependencies
In order to run the project the following library and software are required:

- **Docker** — to host MySQL and MongoDB  
- **Pandas** — for loading and preprocessing CSV data  
- **MongoDB & PyMongo** — database and Python client  
- **MySQL & SQLAlchemy** — database and ORM/connection layer  

A `docker-compose.yml` file is provided for quick setup.

---

## Pipeline / How to Run
### 0. Download the 3 csv files from the kaggle's dataset. You can use the link to find them
### 1. Start Docker
All configuration is provided in the `docker-compose.yml` file. Just need to run in the shell:

```bash
docker compose up
```
### 2. Download the Dataset
Download the following files from the given link of the dataset in Kaggle:
- `movies.csv`
- `credits.csv`
- `ratings.csv`
Place them in the project directory and update the file paths in `preprocessing.py` to match the new path.
---

### 3. Run the Main Script
Execute the main.py file selecting one of the 2 options:

```bash
python main.py
```
#### **Option 1 — First Run (Preprocess + Load Data + Run Queries)**
Choose **1** to:

1. Preprocess the dataset  
2. Load data into **MySQL** and **MongoDB**  
3. Run all benchmark queries:
   - first **without indexes**  
   - then **with indexes**  
4. Save results to:
   - `query_time.csv` — query execution times  
   - `loading_time.csv` — data loading times  

Use this option only on the first run or when resetting the databases. 

---

#### **Option 0 — Subsequent Runs (Run Queries Only)**
Choose **0** to skip preprocessing and data loading.  
Only the benchmark queries will be executed on the already-populated databases.
