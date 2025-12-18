Movie Data Pipeline
Overview
This project is a simple movie data pipeline built for a Data Engineer assignment. The goal of the pipeline is to ingest movie and rating data from local CSV files, enrich it using the OMDb API, clean and transform the data, and load it into a relational database for analytics purposes. The pipeline is built using Python and demonstrates basic ETL concepts, data modeling, and SQL analytics.
Project Structure
movie-data-pipeline/
├── etl.py # Python script to perform ETL operations
├── schema.sql # SQL script to create database tables
├── queries.sql # SQL queries for analytical questions
└── README.md # Project documentation

Environment Setup

Prerequisites
- Python 3.x installed
- A relational database (SQLite, MySQL, or PostgreSQL recommended)
- OMDb API key (get one for free at http://www.omdbapi.com/)

Install Dependencies
Run the following command to install required Python packages:
```bash
pip install -r requirements.txt

Running the Project
1.	Create your database using the SQL statements in schema.sql.
2.	Update your database credentials in etl.py if required.
3.	Run the ETL script to extract, transform, and load the data:
python etl.py
4.	Once the data is loaded, run the SQL queries in queries.sql to answer analytical questions.
Design Choices and Assumptions
•	Database: SQLite was used for simplicity, but the pipeline is compatible with MySQL/PostgreSQL.
•	Data Modeling:
o	Movies table stores movie details including enriched fields from OMDb API.
o	Ratings table stores user ratings linked to movies.
o	Genres are stored as a separate column but can be split for advanced analytics.
•	ETL Logic:
o	Handles missing API responses gracefully.
o	Ensures idempotency: running etl.py multiple times does not create duplicates.
•	Assumptions:
o	Movie titles in the CSV may not always exactly match OMDb API results.
o	Movies not found in the API are loaded with available CSV data only.
Challenges and Solutions
•	API Matching Issues: Some movie titles did not exactly match OMDb API. Solution: Implemented a fallback mechanism to handle missing data.
•	Data Cleaning: Converted date fields, handled missing values, and parsed genres from | separated strings.
•	Idempotency: Ensured that duplicate inserts are avoided by using primary keys and proper database checks.
Future Improvements
•	Implement batch API calls to reduce request time.
•	Normalize genres into a separate table for more flexible analytics.
•	Add logging and error handling for better monitoring in production.
•	Scale the pipeline to handle larger datasets and streaming data.
