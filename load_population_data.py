#ETL population data table
import wbgapi as wb
import mysql.connector
import concurrent.futures
import itertools
import time

iso_codes = ['ABW', 'AFG', 'AGO', 'ALB', 'AND', 'ARE', 'ARG', 'ARM', 'ASM', 'ATG', 'AUS', 
              'AUT', 'AZE', 'BDI', 'BEL', 'BEN', 'BFA', 'BGD', 'BGR', 'BHR', 'BHS', 'BIH', 'BLR', 'BLZ', 'BMU', 'BOL', 'BRA', 
              'BRB', 'BRN', 'BTN', 'BWA', 'CAF', 'CAN', 'CHE', 'CHL', 'CHN', 'CIV', 'CMR', 'COD', 'COG', 'COL', 
              'COM', 'CPV', 'CRI', 'CUB', 'CUW', 'CYM', 'CYP', 'CZE', 'DEU', 'DJI', 'DMA', 'DNK', 'DOM', 'DZA', 'ECU', 'EGY', 
              'ERI', 'ESP', 'EST', 'ETH', 'FIN', 'FJI', 'FRA', 'FRO', 'FSM', 'GAB', 'GBR', 'GEO', 'GHA', 'GIB', 'GIN', 'GMB', 'GNB', 
              'GNQ', 'GRC', 'GRD', 'GRL', 'GTM', 'GUM', 'GUY', 'HKG', 'HND', 'HRV', 'HTI', 'HUN', 'IDN', 'IMN', 'IND', 'IRL', 'IRN', 'IRQ', 
              'ISL', 'ISR', 'ITA', 'JAM', 'JOR', 'JPN', 'KAZ', 'KEN', 'KGZ', 'KHM', 'KIR', 'KNA', 'KOR', 'KWT', 'LAO', 'LBN', 'LBR', 
              'LBY', 'LCA', 'LIE', 'LKA', 'LSO', 'LTU', 'LUX', 'LVA', 'MAC', 'MAF', 'MAR', 'MCO', 'MDA', 'MDG', 'MDV', 'MEX', 'MHL', 
              'MKD', 'MLI', 'MLT', 'MMR', 'MNE', 'MNG', 'MNP', 'MOZ', 'MRT', 'MUS', 'MWI', 'MYS', 'NAM', 'NCL', 'NER', 'NGA', 'NIC', 'NLD', 
              'NOR', 'NPL', 'NRU', 'NZL', 'OMN', 'PAK', 'PAN', 'PER', 'PHL', 'PLW', 'PNG', 'POL', 'PRI', 'PRK', 'PRT', 'PRY', 'PSE', 'PYF', 'QAT', 
              'ROU', 'RUS', 'RWA', 'SAU', 'SDN', 'SEN', 'SGP', 'SLB', 'SLE', 'SLV', 'SMR', 'SOM', 'SRB', 'SSD', 'STP', 'SUR', 'SVK', 'SVN', 'SWE', 'SWZ', 'SXM', 
              'SYC', 'SYR', 'TCA', 'TCD', 'TGO', 'THA', 'TJK', 'TKM', 'TLS', 'TON', 'TTO', 'TUN', 'TUR', 'TUV', 'TZA', 'UGA', 'UKR', 'URY', 'USA', 'UZB', 
              'VCT', 'VEN', 'VGB', 'VIR', 'VNM', 'VUT', 'WSM', 'YEM', 'ZAF', 'ZMB', 'ZWE']

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
    cnx = mysql.connector.connect(user = 'root', port = '3306', 
                              database = 'GDP_ETL_DATA')
    cursor = cnx.cursor()
    cursor.executemany('INSERT INTO Total_Population (CountryID, Total_Population, Year) VALUES (%s, %s, %s)', resList)

    cnx.commit()
    cnx.close()

if __name__ == "__main__":
    resList = transform()
    load(resList)
    end = time.perf_counter()
    print(end - start)