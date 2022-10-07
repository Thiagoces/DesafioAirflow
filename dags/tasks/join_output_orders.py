import pandas as pd
import sqlite3

def contarenvios_rio():
    # Conexão sqlite 
    con = sqlite3.connect('/home/thiagocesar/repos/airflow_tooltorial/data/Northwind_small.sqlite')

    cursor = con.cursor()
    order_datail = pd.read_sql ('select * from "OrderDetail"', con)
    orders = pd.read_csv('output_orders.csv', sep = ',')
  
   # Alterar o nome da coluna ID
    orders = orders.rename(columns={'Id': 'OrderId'}) 

    # Realizar o Join
    unir = pd.merge(order_datail, orders, how='left', on = 'OrderId')

    # Condicional para contar os envios a cidade do Rio de Janeiro
    unir = unir[unir['ShipCity'] == 'Rio de Janeiro']

    # Exportar para o TXT
    somatorio = str(unir['Quantity'].sum())
    print (somatorio)

    # Arquivo count.txt
    with open("count.txt" , "w") as folder:
        folder.write(somatorio)
    
    return somatorio 

