import traceback
import logging
import pandas as pd
import urllib.request
import utils


# url = "https://raw.githubusercontent.com/dogukannulu/datasets/master/Churn_Modelling.csv"


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')


def download_file_from_url(url: str, file_name: str):
    """
    Download a file from a specific URL to a local directory
    """
    try:
        urllib.request.urlretrieve(url, f'data/{file_name}.csv')
        logging.info(' csv file downloaded successfully to the working directory')
    except Exception as e:
        logging.error(f'Error while downloading the csv file due to: {e}')
        traceback.print_exc()


def create_postgres_table(file_name:str):
    """
    Create the Postgres table with a desired schema
    """
    conn = utils.postgres_connection()
    cur = conn.cursor()

    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {file_name} (RowNumber INTEGER PRIMARY KEY, CustomerId INTEGER, 
        Surname VARCHAR(50), CreditScore INTEGER, Geography VARCHAR(50), Gender VARCHAR(20), Age INTEGER, 
        Tenure INTEGER, Balance FLOAT, NumOfProducts INTEGER, HasCrCard INTEGER, IsActiveMember INTEGER, EstimatedSalary FLOAT, Exited INTEGER)""")
        
        logging.info(f' New table {file_name} created successfully to postgres server')
        conn.commit()
    except:
        logging.warning(f' Check if the table {file_name} exists')
        cur.close()
        conn.close()


def write_to_postgres(file_name:str):
    """Create the dataframe and write to Postgres table if it doesn't already exist

    Args:
        file_name (str): name of the file to be written to Postgres
    """
    conn = utils.postgres_connection()
    cur = conn.cursor()

    df = pd.read_csv(f'data/{file_name}.csv')
    inserted_row_count = 0

    for _, row in df.iterrows():
        count_query = f"""SELECT COUNT(*) FROM {file_name} WHERE RowNumber = {row['RowNumber']}"""
        cur.execute(count_query)
        result = cur.fetchone()
        
        if result[0] == 0:
            inserted_row_count += 1
            cur.execute(f"""INSERT INTO {file_name} (RowNumber, CustomerId, Surname, CreditScore, Geography, Gender, Age, 
            Tenure, Balance, NumOfProducts, HasCrCard, IsActiveMember, EstimatedSalary, Exited) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s)""", 
            (int(row[0]), int(row[1]), str(row[2]), int(row[3]), str(row[4]), str(row[5]), int(row[6]), int(row[7]), float(row[8]), int(row[9]), int(row[10]), int(row[11]), float(row[12]), int(row[13])))
    
    logging.info(f' {inserted_row_count} rows from csv file inserted into {file_name} table successfully')
    conn.commit()
    cur.close()
    conn.close()


def main():
    print('Enter the url to download the csv file')
    url = input()

    file_name = url.split('/')[-1].split('.')[0].lower()

    download_file_from_url(url, file_name)
    create_postgres_table(file_name)
    write_to_postgres(file_name)

if __name__ == '__main__':
    main()