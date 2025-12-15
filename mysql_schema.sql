CREATE TABLE movies (
    id INT PRIMARY KEY,
    budget BIGINT,
    title VARCHAR(255),
    original_title VARCHAR(255),
    original_language VARCHAR(10),
    overview TEXT,
    popularity FLOAT,
    release_date DATE,
    revenue BIGINT,
    runtime INT,
    status VARCHAR(50)
);

CREATE TABLE genres (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE
);

CREATE TABLE people (
    person_id INT PRIMARY KEY,
    person_name VARCHAR(255),
    gender INT
);

CREATE TABLE movies_genres (
    movie_id INT,
    genre_id INT,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE ratings (
    user_id INT,
    movie_id INT,
    rating FLOAT,
    PRIMARY KEY(user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies(id)
);

CREATE TABLE actors (
    movie_id INT,
    characters VARCHAR(255),
    person_id INT,
    PRIMARY KEY(movie_id, person_id, characters),
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (person_id) REFERENCES people(person_id)
);

CREATE TABLE crew (
    movie_id INT,
    job VARCHAR(255),
    person_id INT,
    department VARCHAR(255),
    PRIMARY KEY(movie_id, person_id, job),
    FOREIGN KEY (movie_id) REFERENCES movies(id),
    FOREIGN KEY (person_id) REFERENCES people(person_id)
);