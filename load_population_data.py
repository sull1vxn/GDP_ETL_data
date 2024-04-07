#ETL population data table
import wbgapi as wb
import mysql.connector
import concurrent.futures
import itertools
import time

iso_codes = [list of all countries; length is 215]

start = time.perf_counter()



def extract(start, end):
    tupleList = [] #tupleList will hold tuples of data to be appended
    for i in range(start, end):
        for x in range(1980,2023):
            try:
                data = wb.data.DataFrame('SP.POP.TOTL', f'{iso_codes[i]}', f'{x}')
                z = data['SP.POP.TOTL']
                pop = int(z.iloc[0]) #used to convert z(population data) to INT format
                current_data = (i + 1, pop, x) #holds current data for year/country in a tuple
                tupleList.append(current_data) #used to append current_data to a list of tuples
            except Exception as e:
                print(f'Error at country **{iso_codes[i]}** and year **{x}')

    return tupleList


def transform():
    resList = [] #list to append the results of each process; ensures orderly fashion during SQL insertion
    with concurrent.futures.ProcessPoolExecutor() as pool:
        res1 = pool.submit(extract,0,42)
        res2 = pool.submit(extract,43,86)
        res3 = pool.submit(extract,87,130)
        res4 = pool.submit(extract,131,173)
        res5 = pool.submit(extract,174,215)
        resList.extend(itertools.chain(res1.result(), res2.result(), 
                                       res3.result(), res4.result(), res5.result()))
    return resList

def load(resList):
    cnx = mysql.connector.connect(user = 'x', port = 'x', 
                              database = 'x')
    cursor = cnx.cursor()
    cursor.executemany('INSERT INTO Total_Population (CountryID, Total_Population, Year) VALUES (%s, %s, %s)', resList)

    cnx.commit()
    cnx.close()

if __name__ == "__main__":
    resList = transform()
    load(resList)
    end = time.perf_counter()
    print(end - start)
