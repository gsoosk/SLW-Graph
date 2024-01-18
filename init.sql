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



-- Inserting data into Players
INSERT INTO Players (PId, Pname, Pcash) VALUES
    (1, 'Alice', 1000.00),
    (2, 'Bob', 1200.00),
    (3, 'Charlie', 1500.00),
    (4, 'David', 1100.00),
    (5, 'Eve', 1300.00),
    (6, 'Frank', 900.00),
    (7, 'Grace', 950.00),
    (8, 'Hannah', 800.00),
    (9, 'Ivan', 1450.00),
    (10, 'Julia', 1600.00),
    (11, 'Kevin', 1050.00),
    (12, 'Laura', 1150.00),
    (13, 'Mike', 1250.00),
    (14, 'Nora', 1350.00),
    (15, 'Oscar', 1400.00),
    (16, 'Pam', 1550.00),
    (17, 'Quinn', 850.00),
    (18, 'Rachel', 950.00),
    (19, 'Steve', 750.00),
    (20, 'Tina', 1750.00);

-- Inserting data into Items
INSERT INTO Items (IId, IName, IOwner) VALUES
    (1, 'Sword', 1),
    (2, 'Shield', 2),
    (3, 'Bow', 3),
    (4, 'Arrow', 4),
    (5, 'Helmet', 5),
    (6, 'Gloves', 6),
    (7, 'Boots', 7),
    (8, 'Ring', 8),
    (9, 'Necklace', 9),
    (10, 'Bracelet', 10),
    (11, 'Potion', 11),
    (12, 'Amulet', 12),
    (13, 'Cape', 13),
    (14, 'Staff', 14),
    (15, 'Robe', 15),
    (16, 'Boots of Speed', 16),
    (17, 'Magic Wand', 17),
    (18, 'Spell Book', 18),
    (19, 'Crystal Ball', 19),
    (20, 'Cloak of Invisibility', 20),
    (21, 'Battle Axe', 1),
    (22, 'Warhammer', 2),
    (23, 'Longsword', 3),
    (24, 'Dagger', 4),
    (25, 'Crossbow', 5),
    (26, 'Magic Scroll', 6),
    (27, 'Quiver', 7),
    (28, 'Gauntlets', 8),
    (29, 'Orb of Power', 9),
    (30, 'Chainmail Armor', 10),
    (31, 'Silver Ring', 11),
    (32, 'Golden Necklace', 12),
    (33, 'Iron Shield', 13),
    (34, 'Leather Boots', 14),
    (35, 'Elven Cloak', 15),
    (36, 'Wizard Hat', 16),
    (37, 'Healing Herbs', 17),
    (38, 'Phoenix Feather', 18),
    (39, 'Dragon Scale', 19),
    (40, 'Goblin Ear', 20),
    (41, 'Griffin Claw', 1),
    (42, 'Unicorn Horn', 2),
    (43, 'Elf Bow', 3),
    (44, 'Dwarven Axe', 4),
    (45, 'Knight Armor', 5),
    (46, 'Rogue Dagger', 6),
    (47, 'Priest Robe', 7),
    (48, 'Paladin Sword', 8),
    (49, 'Necromancer Staff', 9),
    (50, 'Ranger Boots', 10);

-- Inserting data into Listings
INSERT INTO Listings (LId, LIId, LPrice) VALUES
    (1, 1, 200.00),
    (2, 5, 150.00),
    (3, 10, 300.00),
    (4, 15, 250.00),
    (5, 20, 350.00),
    (6, 25, 400.00),
    (7, 30, 450.00),
    (8, 35, 500.00),
    (9, 40, 550.00),
    (10, 45, 600.00);
