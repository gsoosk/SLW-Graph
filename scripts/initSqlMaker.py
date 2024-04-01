from random import randint
import string
from tqdm import tqdm
import random

num_of_players = 500000
num_of_each_player_items = 5
num_of_initial_listings = 100000
player_cash_range = (100, 1000)
item_price_range = (20, 200)
name_length = 8

text_file = open("init.sql", "w")
N = name_length




tables_query = '''
CREATE TABLE IF NOT EXISTS Players (
    PId INT,
    Pname VARCHAR(255),
    Pcash DECIMAL(10, 2),
    PRIMARY KEY (PId)
);

CREATE TABLE IF NOT EXISTS Items (
    IId INT,
    IName VARCHAR(255),
    IOwner INT,  -- Assuming this references PId in Players
    PRIMARY KEY (IId),
    FOREIGN KEY (IOwner) REFERENCES Players(PId)
);

CREATE TABLE IF NOT EXISTS Listings (
    LId INT PRIMARY KEY,
    LIId INT,  -- Assuming this references IId in Items
    LPrice DECIMAL(10, 2),
    FOREIGN KEY (LIId) REFERENCES Items(IId)
);
'''

text_file.write(tables_query)


insert_players = '''
INSERT INTO Players (PId, Pname, Pcash) VALUES'''

insert_items = '''
INSERT INTO Items (IId, IName, IOwner) VALUES'''

insert_listings = '''
INSERT INTO Listings (LId, LIId, LPrice) VALUES'''

def shuffle(n):
    lst = list(range(n))
    random.shuffle(lst)
    return lst

def randomString():
    return ''.join(random.choices(string.ascii_uppercase + string.digits + string.ascii_letters, k=N))


text_file.write(insert_players)
print(f'Generating {num_of_players} players')
pbar = tqdm(total=num_of_players, desc="Players")
shuffled_players = shuffle(num_of_players)
for i in range(num_of_players):
    pbar.update(1)
    text_file.write(f'''
    ({shuffled_players[i]}, 'P{randomString()}', {randint(player_cash_range[0], player_cash_range[1])}){';' if i == num_of_players - 1 else ','}''')
pbar.close()


text_file.write(insert_items)
print(f'Generating {num_of_players * num_of_each_player_items} items')
pbar = tqdm(total=num_of_players * num_of_each_player_items, desc="Items")
shuffled_items = shuffle(num_of_players * num_of_each_player_items)
for item in range(num_of_each_player_items):
    shuffled_players = shuffle(num_of_players)
    for i in range(num_of_players):
        text_file.write(f'''
    ({shuffled_items[item * num_of_players + i]},'I{randomString()}',{shuffled_players[i]}){';' if item == num_of_each_player_items - 1 and i == num_of_players - 1 else ','}''')
        pbar.update(1)
pbar.close()


text_file.write(insert_listings)
shuffled_listings = shuffle(num_of_initial_listings)
random_items = random.sample(range(num_of_players * num_of_each_player_items), num_of_initial_listings)
for i in tqdm(range(num_of_initial_listings), desc="Listings"):
    text_file.write(f'''
    ({shuffled_listings[i]},{random_items[i]},{randint(item_price_range[0], item_price_range[1])}){';' if i == num_of_initial_listings - 1 else ','}''')
