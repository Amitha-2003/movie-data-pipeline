USE movie_pipeline;
SELECT COUNT(*) AS total_movies FROM movies;
SELECT COUNT(*) AS total_ratings FROM ratings;

/*1.	Which movie has the highest average rating?*/

SELECT m.title, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC
LIMIT 1;


/* 2.	What are the top 5 movie genres that have the highest average rating? */
SELECT genre, AVG(r.rating) AS avg_rating
FROM ratings r
JOIN movies m ON r.movie_id = m.movie_id
JOIN (
    SELECT movie_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(genres, '|', n.n), '|', -1)) AS genre
    FROM movies
    CROSS JOIN (
        SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8
    ) n
    WHERE n.n <= (LENGTH(genres) - LENGTH(REPLACE(genres, '|', '')) + 1)
) genre_table ON m.movie_id = genre_table.movie_id
GROUP BY genre
ORDER BY avg_rating DESC
LIMIT 5;

/*3.	Who is the director with the most movies in this dataset?*/

SELECT director, COUNT(*) AS num_movies
FROM movies
WHERE director IS NOT NULL
GROUP BY director
ORDER BY num_movies DESC
LIMIT 1;

/* 4.	What is the average rating of movies released each year?*/
SELECT m.release_year, AVG(r.rating) AS avg_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;



