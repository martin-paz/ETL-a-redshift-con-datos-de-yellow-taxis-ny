import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

#funcion para calcular outliers
def outliers_obt(data, columna,cuartial1,cuartil2,valoriqr=1.5):
    ##calculamos los cuartiles 
    Q1 = data[columna].quantile(float(cuartial1))
    #print('Primer Cuartile', Q1)
    Q3 = data[columna].quantile(float(cuartil2))
    #print('Tercer Cuartile',Q3)
    IQR = Q3 - Q1
    #print('Rango intercuartile', IQR)

    ##calculamos los bigotes superior e inferior
    BI = (Q1 - valoriqr * IQR)
    #print('bigote Inferior \n', BI)
    BS = (Q3 + valoriqr * IQR)
    #print('bigote superior \n', BS)

    ##obtenemos una nueva tabla sin los outliers
    ubi_sin_out = data[(data[columna] >= BI) & (data[columna] <= BS)]
    return ubi_sin_out

def extract_directory(directorio):
    data=pd.read_parquet(directorio)
    return data

def create_df_dicc(dictionary, colums):
    df = pd.DataFrame([[key, dictionary[key]] for key in dictionary.keys()], columns=colums)
    return df

def conect_db(url,data_base,port,user,pwd):
    urls=url
    data_bases=data_base
    ports=port
    users=user
    pwds=pwd
    try:
        conn = psycopg2.connect(
            host=urls,
            dbname=data_bases,
            user=users,
            password=pwds,
            port=ports
        )
        print("Connected to Redshift successfully!")
        
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
    
    return conn

###Insertar Datos##
def cargar_en_redshift(conn, table_name,type_data, dataframe):
        dtypes= dataframe.dtypes
        cols= list(dtypes.index )
        tipos= list(dtypes.values)
        type_map = type_data
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
        
        # Combine column definitions into the CREATE TABLE statement
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
        table_schema = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_defs)}
            );
            """
        print(table_name)
        #Crear la tabla
        cur = conn.cursor()
        cur.execute(table_schema)
        # Generar los valores a insertar
        values = [tuple(x) for x in dataframe.to_numpy()]
        # Definir el INSERT
        insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
        
        # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso terminado')