import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import streamlit_shadcn_ui as ui
import plotly.express as px

st.markdown("# Evolución de ventas")
st.markdown("---")

meses = {
    "Enero": "01",
    "Febrero": "02",
    "Marzo": "03",
    "Abril": "04",
    "Mayo": "05",
    "Junio": "06",
    "Julio": "07",
    "Agosto": "08",
    "Septiembre": "09",
    "Octubre": "10",
    "Noviembre": "11",
    "Diciembre": "12"
}


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

    

# Recaudaciones
def tp_ingresos():
    engine = connect_to_db()
    query = """
    SELECT
	QRY_INGRESOS_TOTALES_PBI.date,
	CONCAT(YEAR(QRY_INGRESOS_TOTALES_PBI.date), '-', LPAD(MONTH(QRY_INGRESOS_TOTALES_PBI.date), 2, '0')) AS period,	
    YEAR(QRY_INGRESOS_TOTALES_PBI.date) AS year,
    CASE MONTH(QRY_INGRESOS_TOTALES_PBI.date)
    WHEN 1 THEN '01 - Enero'
    WHEN 2 THEN '02 - Febrero'
    WHEN 3 THEN '03 - Marzo'
    WHEN 4 THEN '04 - Abril'
    WHEN 5 THEN '05 - Mayo'
    WHEN 6 THEN '06 - Junio'
    WHEN 7 THEN '07 - Julio'
    WHEN 8 THEN '08 - Agosto'
    WHEN 9 THEN '09 - Septiembre'
    WHEN 10 THEN '10 - Octubre'
    WHEN 11 THEN '11 - Noviembre'
    WHEN 12 THEN '12 - Diciembre'
    END AS periodo,
	QRY_INGRESOS_TOTALES_PBI.branch_office_id,
	QRY_INGRESOS_TOTALES_PBI.clave,
	QRY_INGRESOS_TOTALES_PBI.ticket_number,
	QRY_INGRESOS_TOTALES_PBI.abonados,
	QRY_INGRESOS_TOTALES_PBI.net_amount,
	QRY_INGRESOS_TOTALES_PBI.transbank
    FROM
	QRY_INGRESOS_TOTALES_PBI;
    """
    df_collections = pd.read_sql(query, engine)
    return df_collections


# Presupuesto
def qry_ppto_dia():
    engine = connect_to_db()
    query = """
    SELECT 
    date,
    YEAR(date)as year,
	CONCAT(YEAR(date), '-', LPAD(MONTH(date), 2, '0')) AS period,
	CASE MONTH(date)
    WHEN 1 THEN '01 - Enero'
    WHEN 2 THEN '02 - Febrero'
    WHEN 3 THEN '03 - Marzo'
    WHEN 4 THEN '04 - Abril'
    WHEN 5 THEN '05 - Mayo'
    WHEN 6 THEN '06 - Junio'
    WHEN 7 THEN '07 - Julio'
    WHEN 8 THEN '08 - Agosto'
    WHEN 9 THEN '09 - Septiembre'
    WHEN 10 THEN '10 - Octubre'
    WHEN 11 THEN '11 - Noviembre'
    WHEN 12 THEN '12 - Diciembre'
    END AS periodo,
    branch_office_id,
    ppto		
    FROM 
    QRY_PPTO_DIA"""
    ppto = pd.read_sql(query, engine)
    return ppto 

# Sucursales
def qry_branch_offices():
    engine = connect_to_db()
    query = "SELECT * FROM QRY_BRANCH_OFFICES WHERE status_id = 15"
    sucursales = pd.read_sql(query, engine)
    return sucursales 

# Same Store Sale
def qry_sss():
    engine = connect_to_db()
    query = "SELECT * FROM QRY_SSS"
    sss = pd.read_sql(query, engine)
    return sss 

# Periodos
def generar_periodos(mes_actual):
   periodos = ['01-Enero','02-Febrero','03-Marzo','04-Abril','05-Mayo','06-Junio',
              '07-Julio','08-Agosto','09-Septiembre','10-Octubre','11-Noviembre','12-Diciembre']
   periodos[mes_actual-1] = 'Acumulado'
   return periodos   


# Main
def main(authenticated=True):    
    if not authenticated:
        raise Exception("No autenticado, Necesitas autenticarte primero")
        #return
        #st.error("Necesitas autenticarte primero")
        #return
    else:
        
        df_ingresos = tp_ingresos()
        df_sucursales = qry_branch_offices()
        df_ppto = qry_ppto_dia()
        df_sss = qry_sss() 
             
        ### INGRESOS ACTUAL 2024
        df_ingresos_2024 = df_ingresos[(df_ingresos["year"] == 2024)]
        columns_ingresos = ["periodo", "branch_office_id" , "ticket_number" , "net_amount" , "transbank" , "abonados" , "clave"]
        df_ingresos_act = df_ingresos_2024[columns_ingresos]
    
        df_ingresos_act = df_ingresos_act.rename(columns={"ticket_number": "ticket_number_Act", 
                                            "net_amount" : "Venta_Neta_Act" ,
                                            "transbank": "Transbank_Act",
                                            "abonados" : "Abonados_Act"})        
        #st.dataframe(df_ingresos_act)
        
        
        ### INGRESOS ANTERIOR 2023
        df_ingresos_2023 = df_ingresos[(df_ingresos["year"] == 2023)]
        columns_ingresos = ["periodo" ,"date",  "branch_office_id" , "ticket_number" , "net_amount" , "transbank" , "abonados", "clave" ]
        df_ingresos_ant = df_ingresos_2023[columns_ingresos]
    
        df_ingresos_ant = df_ingresos_ant.rename(columns={"ticket_number": "ticket_number_Ant", 
                                            "net_amount" : "Venta_Neta_Ant" ,
                                            "transbank": "Transbank_Ant",
                                            "abonados" : "Abonados_Ant"})        
        #st.dataframe(df_ingresos_ant)
        
        df_general = df_ingresos_act.merge(df_ingresos_ant, on=["branch_office_id", "periodo"], how="outer", suffixes=('_Act', '_Ant'))
        
        df_general = df_general.fillna(0)  
        df_general = df_general.groupby(["periodo","date", "branch_office_id"]).first().reset_index()   
        st.dataframe(df_general)    
        
        df_general = df_general.merge(df_ppto[['branch_office_id', 'date', 'ppto']], on=["branch_office_id", "date"], how="outer")
        
 
        st.dataframe(df_general) 
        
        
if __name__ == "__main__":
    main()    

        
        

