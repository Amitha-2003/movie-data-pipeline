CREATE DATABASE IF NOT EXISTS movie_pipeline;
USE movie_pipeline;

CREATE TABLE IF NOT EXISTS movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year INT,
    genres VARCHAR(255),
    director VARCHAR(255),
    runtime_minutes INT,
    imdb_rating DECIMAL(3,1)
);

CREATE TABLE IF NOT EXISTS ratings (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating DECIMAL(2,1),
    rating_timestamp BIGINT,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);
