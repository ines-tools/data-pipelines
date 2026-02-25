using SQLite

# create new database
db = SQLite.DB("biomass.db")

# create new table in database

SQLite.execute(db, "CREATE TABLE IF NOT EXISTS biomass_mass
        (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, breed TEXT, email TEXT)
")


SQLite.execute(db, "INSERT INTO biomass_mass (name, age, breed, email) 
        VALUES ('some name', 57, 'vfewav', 'vre@vfe.com')
")