import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Conexión a la base de datos MySQL
def connect_to_db():
    db_config = {
        'host': '216.137.190.82',
        'user': 'jysparki_admin',
        'password': 'Admin2024$',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

meses = {
    1: '01-Enero',
    2: '02-Febrero',
    3: '03-Marzo',
    4: '04-Abril',
    5: '05-Mayo',
    6: '06-Junio',
    7: '07-Julio',
    8: '08-Agosto',
    9: '09-Septiembre',
    10: '10-Octubre',
    11: '11-Noviembre',
    12: '12-Diciembre'
}

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
    
    # Actualiza la columna 'periodo' en el dataframe df_viejos
    df_viejos['periodo'] = df_viejos['fecha'].dt.month.map(meses)

    # Elimina los registros de 30 días hacia atrás
    limite_fecha = pd.Timestamp.now() - pd.Timedelta(days=30)
    df_viejos = df_viejos[df_viejos['fecha'] < limite_fecha]

    # Agrega los nuevos registros obtenidos del SQL
    df_completos = pd.concat([df_viejos, df_nuevos])
    # Guarda los datos en el archivo CSV
    df_completos.to_csv(nombre_archivo, index=False)


def api_transactions():
    engine = connect_to_db()
    query = """
    SELECT
    QRY_BRANCH_OFFICES.branch_office,
    api_transactions.folio,
    SUM((api_transactions.total)*1) AS total,
    api_transactions.entrance_hour,
    api_transactions.exit_hour,
    TIME_FORMAT(TIMEDIFF(api_transactions.exit_hour, api_transactions.entrance_hour), '%H:%i:%s') AS estancia,
    HOUR(api_transactions.exit_hour) AS hora,
    DATE_FORMAT(api_transactions.created_at,'%Y-%m-%d') AS fecha
    FROM api_transactions
    LEFT JOIN cashiers
    ON api_transactions.cashier_id = cashiers.cashier_id
    LEFT JOIN QRY_BRANCH_OFFICES
    ON api_transactions.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
    WHERE
    api_transactions.created_at >= DATE(NOW()) - INTERVAL 30 DAY AND YEAR(api_transactions.created_at) = YEAR(curdate())
    AND QRY_BRANCH_OFFICES.status_id = 15 AND cashiers.is_electronic_id = 1
    GROUP BY
    QRY_BRANCH_OFFICES.branch_office,
    api_transactions.folio,
    DATE_FORMAT(api_transactions.created_at,'%Y-%m-%d')
    ORDER BY DATE_FORMAT(api_transactions.created_at,'%Y-%m-%d')
    """    
    df_api_transactions = pd.read_sql(query, engine)    
    # Guardar el dataframe en un archivo CSV
    nombre_archivo = 'archivos/api_transactions.csv'
    guardar_datos_csv(df_api_transactions, nombre_archivo)
    return df_api_transactions

def dte_transactions():
    engine = connect_to_db()
    query = """
    SELECT
    QRY_BRANCH_OFFICES.branch_office,
    dte_transactions.folio,
    SUM((dte_transactions.total)*1) AS total,
    dte_transactions.entrance_hour,
    dte_transactions.exit_hour,
    TIME_FORMAT(TIMEDIFF(dte_transactions.exit_hour, dte_transactions.entrance_hour), '%H:%i:%s') AS estancia,
    HOUR(dte_transactions.exit_hour) AS hora,
    DATE_FORMAT(dte_transactions.created_at,'%Y-%m-%d') AS fecha
    FROM dte_transactions
    LEFT JOIN cashiers
    ON dte_transactions.cashier_id = cashiers.cashier_id
    LEFT JOIN QRY_BRANCH_OFFICES
    ON dte_transactions.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
    WHERE
    dte_transactions.created_at >= DATE(NOW()) - INTERVAL 30 DAY AND YEAR(dte_transactions.created_at) = YEAR(curdate())
    AND QRY_BRANCH_OFFICES.status_id = 15 AND cashiers.is_electronic_id = 1
    GROUP BY
    QRY_BRANCH_OFFICES.branch_office,
    dte_transactions.folio,
    DATE_FORMAT(dte_transactions.created_at,'%Y-%m-%d')
    ORDER BY DATE_FORMAT(dte_transactions.created_at,'%Y-%m-%d')
    """
    
    df_dte_transactions = pd.read_sql(query, engine)    
    # Guardar el dataframe en un archivo CSV
    nombre_archivo = 'archivos/dte_transactions.csv'
    guardar_datos_csv(df_dte_transactions, nombre_archivo)
    return df_dte_transactions

def qry_branch_offices():
    engine = connect_to_db()
    query = "SELECT * FROM QRY_BRANCH_OFFICES WHERE status_id = 15"
    sucursales = pd.read_sql(query, engine)
    return sucursales

def main(authenticated=True):
    if not authenticated:
        raise Exception("No autenticado, Necesitas autenticarte primero")        
    else:
        st.title("INFORME DE VENTAS X HORAS")
        df_dte_transactions = dte_transactions()
        df_api_transactions = api_transactions()
        df_sucursales = qry_branch_offices()

        frames = [df_dte_transactions, df_api_transactions]
        result = pd.concat(frames)
              
        st.title("Merge Venta X Hora")
        st.dataframe(result)
        st.title("Sucursales")
        st.dataframe(df_sucursales)
        
        

if __name__ == "__main__":
    main()
