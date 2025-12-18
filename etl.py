#!/usr/bin/env python
# coding: utf-8

# In[24]:


pip install pandas requests mysql-connector-python


# In[25]:


import pandas as pd
import requests
import mysql.connector

print("All libraries imported successfully")


# In[26]:


movies_df = pd.read_csv("../data/movies.csv")


# In[9]:


movies_df.head()


# In[10]:


movies_df.info()


# In[21]:


ratings_df = pd.read_csv("../data/ratings.csv")


# In[12]:


ratings_df.head()


# In[13]:


ratings_df.info()


# In[14]:


print("Movies rows:", movies_df.shape[0])
print("Ratings rows:", ratings_df.shape[0])


# In[15]:


print("Missing movieId in movies:", movies_df['movieId'].isna().sum())
print("Missing movieId in ratings:", ratings_df['movieId'].isna().sum())


# In[16]:


ratings_df['rating'].describe()


# In[17]:


invalid_ratings = ratings_df[~ratings_df['movieId'].isin(movies_df['movieId'])]
print("Ratings with invalid movieId:", invalid_ratings.shape[0])


# In[27]:


movies_df['release_year'] = movies_df['title'].str.extract(r'\((\d{4})\)')


# In[28]:


movies_df['release_year'] = movies_df['release_year'].astype('Int64')


# In[35]:


movies_df[['title', 'release_year']].head(10)


# In[36]:


movies_df['release_year'].isna().sum()


# In[1]:


with open("../omdb_api_key.txt", "r") as file:
    OMDB_API_KEY = file.read().strip()

print("API key loaded successfully")


# In[2]:


len(OMDB_API_KEY) 


# In[3]:


import requests

test_title = "Toy Story"

url = "http://www.omdbapi.com/"
params = {
    "t": test_title,
    "apikey": OMDB_API_KEY
}

response = requests.get(url, params=params)
movie_data = response.json()

movie_data


# In[4]:


def fetch_omdb_details(title, api_key):
    url = "http://www.omdbapi.com/"
    params = {
        "t": title,
        "apikey": api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        data = response.json()
        
        if data.get("Response") == "True":
            director = data.get("Director")
            
            runtime = data.get("Runtime")
            if runtime and runtime != "N/A":
                runtime_minutes = int(runtime.split()[0])
            else:
                runtime_minutes = None
            
            imdb_rating = data.get("imdbRating")
            if imdb_rating and imdb_rating != "N/A":
                imdb_rating = float(imdb_rating)
            else:
                imdb_rating = None
            
            return director, runtime_minutes, imdb_rating
        
        else:
            return None, None, None
    
    except Exception as e:
        return None, None, None


# In[5]:


fetch_omdb_details("Toy Story", OMDB_API_KEY) # TEST CODE


# In[11]:


movies_df['director'] = None
movies_df['runtime_minutes'] = None
movies_df['imdb_rating'] = None


# In[12]:


sample_movies = movies_df.head(10)
sample_movies[['title']]


# In[13]:


for idx, row in sample_movies.iterrows():
    director, runtime, imdb_rating = fetch_omdb_details(row['title'], OMDB_API_KEY)
    
    movies_df.at[idx, 'director'] = director
    movies_df.at[idx, 'runtime_minutes'] = runtime
    movies_df.at[idx, 'imdb_rating'] = imdb_rating


# In[14]:


movies_df.head(10)[['title', 'director', 'runtime_minutes', 'imdb_rating']]


# In[15]:


import time


# In[17]:


enriched_count = 0

for idx, row in movies_df.iterrows():
    if enriched_count >= 300:
        break
    
    if movies_df.at[idx, 'director'] is None:
        director, runtime, imdb_rating = fetch_omdb_details(row['title'], OMDB_API_KEY)
        
        movies_df.at[idx, 'director'] = director
        movies_df.at[idx, 'runtime_minutes'] = runtime
        movies_df.at[idx, 'imdb_rating'] = imdb_rating
        
        enriched_count += 1
        time.sleep(0.2)  # avoid hitting API too fast

print("Total movies enriched:", enriched_count)


# In[18]:


movies_df[['director', 'runtime_minutes', 'imdb_rating']].notna().sum()


# In[22]:


movies_db_df = movies_df.rename(columns={"movieId": "movie_id"})

ratings_db_df = ratings_df.rename(columns={
    "userId": "user_id",
    "movieId": "movie_id"
})


# In[31]:


movies_db_df = movies_df.rename(columns={"movieId": "movie_id"})


# In[37]:


print(movies_df.columns.tolist())


# In[38]:


movies_df['director'] = movies_df.get('director')
movies_df['runtime_minutes'] = movies_df.get('runtime_minutes')
movies_df['imdb_rating'] = movies_df.get('imdb_rating')


# In[39]:


movies_df[['director', 'runtime_minutes', 'imdb_rating']].head()


# In[40]:


movies_db_df = movies_df.rename(columns={"movieId": "movie_id"})

movies_db_df = movies_db_df[
    ["movie_id", "title", "release_year", "genres", "director", "runtime_minutes", "imdb_rating"]
]


# In[41]:


pip install mysql-connector-python


# In[43]:


import mysql.connector
print("MySQL connector installed successfully!")


# In[44]:


import mysql.connector

# Replace the placeholders with your actual database credentials
conn = mysql.connector.connect(
    host="localhost",      # or your MySQL server IP
    user="root",
    password="Amitha@2003",
    database="movie_pipeline"
)

# Check if connection was successful
if conn.is_connected():
    print("Connected to MySQL database successfully!")

# Create a cursor object to execute queries
cursor = conn.cursor()


# In[45]:


# Example: Create a table
create_table_query = """
CREATE TABLE IF NOT EXISTS students (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
)
"""
cursor.execute(create_table_query)
print("Table created successfully!")

# Example: Insert data
insert_query = "INSERT INTO students (name, age) VALUES (%s, %s)"
data = ("Junu", 22)
cursor.execute(insert_query, data)
conn.commit()  # Commit changes to the database
print("Data inserted successfully!")

# Example: Retrieve data
select_query = "SELECT * FROM students"
cursor.execute(select_query)
rows = cursor.fetchall()
for row in rows:
    print(row)


# In[46]:


# Close the cursor
cursor.close()

# Close the connection
conn.close()

print("MySQL connection closed successfully!")


# In[47]:


try:
    # Your database operations here
    pass
finally:
    cursor.close()
    conn.close()
    print("Connection closed in finally block.")


# In[ ]:




