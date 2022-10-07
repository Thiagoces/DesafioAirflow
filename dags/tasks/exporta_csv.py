import pandas as pd
import sqlite3

def exporta_csv():
    # Conectar banco de dados
    con = sqlite3.connect('/home/thiagocesar/repos/airflow_tooltorial/data/Northwind_small.sqlite')
    df = pd.read_sql_query('select * from "order"', con)
    
    # Saida do CSV     
    df.to_csv('output_orders.csv', index = False)

    con.close()

