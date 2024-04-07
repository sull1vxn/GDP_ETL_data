import concurrent.futures
import mysql.connector
import itertools



def load(resList):
    cnx = mysql.connector.connect(user = 'root', port = '3306', 
                              database = 'GDP_ETL_DATA')
    cursor = cnx.cursor()
    cursor.executemany('INSERT INTO Population_Table (CountryID, Population, Year) VALUES (%s, %s, %s)', resList)

    cnx.commit()
    cnx.close()

load([(1,1200,2000)])


# create new database in sql tmmr 
# add a table that only takes in integers
# try and use this mp_practice file to practice successfully looping over 8000 integers
# and committing these integers to the practice table




