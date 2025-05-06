import psycopg2
import json
import random
import datetime

# Connect to your PostgreSQL server
conn = psycopg2.connect(
    dbname="lol",      
    user="postgres",        
    password="saim123123",  
    host="localhost",       
    port="5432"             
)

cur = conn.cursor()



# Step 2: Generate a random JSON object
sample_json = {
    "user_id": random.randint(1000, 9999),
    "username": random.choice(["alice", "bob", "charlie"]),
    "active": random.choice([True, False]),
    "score": round(random.uniform(0, 100), 2),
    "timestamp": datetime.datetime.now().isoformat()
}

# Step 3: Insert the JSON object

# Step 4: Commit and query
conn.commit()

cur.execute("SELECT * FROM json_store")
rows = cur.fetchall()
for row in rows:
    print(row)

# Step 5: Clean up
cur.close()
conn.close()
