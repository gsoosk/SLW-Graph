import sqlite3
import os
import random
from random import randint

HOT_RECORDS_PLAYERS = [64, 128, 256, 512, 1024, 2048, 4096]

num_of_players = 500000
num_of_initial_listings = 100000

db = 'test.db'
if os.path.exists(db): os.remove(db)


connection = sqlite3.connect(db)


with open('init.sql', 'r') as sql_file:
    sql_script = sql_file.read()


# init db
cursor = connection.cursor()
cursor.executescript(sql_script)
connection.commit()
print('initiated the db')

cursor = connection.cursor()

for num in HOT_RECORDS_PLAYERS: 
    # select players
    random_players = random.sample(range(num_of_players), num)
    random_players = [str(i) for i in random_players]
    random_players_str = "('" + "','".join(random_players) + "')"

    cursor.execute("SELECT * FROM items WHERE iowner in " + random_players_str)
    items_rows = cursor.fetchall()

    
    item_ids = []
    for item in items_rows: 
        item_ids.append(str(item[0]))

    cursor.execute("SELECT * FROM listings WHERE liid in " + "('" + "','".join(item_ids) + "')")
    listing_rows = cursor.fetchall()

    with open(f'hot_records_{num}_items', 'w') as items_file:
        items_file.write(('\n'.join([str(i[0]) + ',' + str(i[2]) for i in items_rows])))
    
    with open(f'hot_records_{num}_listings', 'w') as listings_file:
        listings_file.write(('\n'.join([str(l[0]) + ',' + str(l[1]) + ',' + str(l[2]) for l in listing_rows])))
    


    

    
