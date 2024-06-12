import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import Error

# Conexión a la base de datos MySQL
def connect_to_db():
    db_config = {
        'host': '103.72.78.28',
        'user': 'jysparki_jis',
        'password': 'Jis2020!',
        'database': 'jysparki_jis'
    }
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine


def get_db_connection():
    try:
        connection_params = {
            'host': '103.72.78.28',
            'user': 'jysparki_jis',
            'password': 'Jis2020!',
            'database': 'jysparki_jis',
            'charset': 'utf8',
            'collation': 'utf8_general_ci',
        }
        conn = mysql.connector.connect(**connection_params)
        return conn
    except Error as e:
        st.write(f"The error '{e}' occurred")

def leer_datos_csv(nombre_archivo):
    df = pd.read_csv(nombre_archivo)
    return df

def guardar_datos_csv(df_nuevos, nombre_archivo):
    try:
        df_viejos = leer_datos_csv(nombre_archivo)
    except:
        df_viejos = pd.DataFrame(columns=df_nuevos.columns)                
    # Convierte la columna 'fecha' a tipo de datos datetime
    df_viejos['fecha'] = pd.to_datetime(df_viejos['fecha'])
    df_nuevos['fecha'] = pd.to_datetime(df_nuevos['fecha'])
    


    # Elimina los registros de 120 días hacia atrás
    limite_fecha = pd.Timestamp.now() - pd.Timedelta(days=120)
    df_viejos = df_viejos[df_viejos['fecha'] < limite_fecha]

    # Agrega los nuevos registros obtenidos del SQL
    df_completos = pd.concat([df_viejos, df_nuevos])
    # Guarda los datos en el archivo CSV
    df_completos.to_csv(nombre_archivo, index=False)


def update_abonados(conn):
    cursor = None
    try:       
        cursor = conn.connection.cursor()  # Aquí se corrige el error
        # Eliminar registros
        query_delete = "DELETE FROM TP_ABONADOS;"
        cursor.execute(query_delete)        
        # Insertar registros
        query = """
        INSERT INTO TP_ABONADOS
        SELECT DISTINCT
            tp_dtes.dte_id AS dte_id, 
            DATE_FORMAT(tp_dtes.created_at,"%Y-%m-%d") AS date, 
            tp_dtes.rut AS rut, 
            UPPER(users.`names`) AS cliente, 
            CONCAT_WS(" - ",tp_dtes.rut,UPPER(users.`names`)) AS razon_social, 
            tp_dtes.folio AS folio, 
            tp_dtes.branch_office_id AS branch_office_id, 
            tp_dtes.dte_type_id AS dte_type_id, 
            tp_dtes.status_id AS status_id, 
            tp_dtes.amount AS amount, 
            tp_dtes.period AS period, 
            tp_dtes.`comment` AS `comment`, 
            statuses.`status` AS `status`, 
            tp_dtes.chip_id AS chip_id
        FROM tp_dtes
            LEFT JOIN customers
            ON tp_dtes.rut = customers.rut
            LEFT JOIN users
            ON customers.rut = users.rut
            LEFT JOIN statuses
            ON tp_dtes.status_id = statuses.status_id
        WHERE
            tp_dtes.rut <> '66666666-6' AND
            tp_dtes.dte_version_id = 1 AND
            tp_dtes.status_id > 17 AND
            tp_dtes.status_id < 24 AND
            users.rol_id = 14 AND
            YEAR(tp_dtes.created_at) = (YEAR(curdate()));
        """
        
        df_nuevos = pd.read_sql_query(query, conn)
        return df_nuevos
    except Error as e:
        st.write(f"The error '{e}' occurred")
        conn.rollback()
    finally:
        if cursor is not None:
            cursor.close()
        
        
def main(authenticated=True):
    if not authenticated:
        raise Exception("No autenticado, Necesitas autenticarte primero")
    else:
        st.title("Carga DTE TP_Abonados")

        # Crea la conexión a la base de datos
        engine = connect_to_db()
        conn = engine.connect()

        # Obtiene los datos desde MySQL
        df_nuevos = update_abonados(conn)

        # Guarda los datos en el archivo CSV
        nombre_archivo = 'tp_abonados.csv'
        guardar_datos_csv(df_nuevos, nombre_archivo)

        # Cierra la conexión a la base de datos
        conn.close()  # Aquí se mueve la línea
        engine.dispose()
        

if __name__ == "__main__":
    main()