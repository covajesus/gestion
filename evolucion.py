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

def tp_recaudacion_neto():
    engine = connect_to_db()
    query = """
    SELECT
        date_format(`collections`.`created_at`, '%Y-%m-%d') AS `date`,
        MONTH(`collections`.`created_at`)as mes,
        YEAR(`collections`.`created_at`)as year,
        `collections`.`branch_office_id` AS `branch_office_id`,
        CONCAT(`collections`.`branch_office_id`, date_format(`collections`.`created_at`, '%Y'),date_format(`collections`.`created_at`, '%m') * 1) AS `clave`,
        SUM(`collections`.`ticket_number`) AS `ticket_number`,
        0 AS `abonados`,
        SUM(`collections`.`net_amount`) AS `net_amount`,
        SUM(`collections`.`card_net_amount`) AS `transbank`
    FROM `collections`
    LEFT JOIN `cashiers`
        ON `collections`.`cashier_id` = `cashiers`.`cashier_id`
    LEFT JOIN `branch_offices`
        ON `collections`.`branch_office_id` = `branch_offices`.`branch_office_id`
    WHERE `cashiers`.`cashier_type_id` <> 3
            AND `collections`.`created_at` >= '2019-12-31'
            AND `branch_offices`.`principal` <> 'ADMINISTRACION'
    GROUP BY
        `collections`.`branch_office_id`,
        `collections`.`created_at`;
    """
    ingresos_neto = pd.read_sql(query, engine)
    return ingresos_neto


def tp_abonados_neto():
    engine = connect_to_db()
    query = """
    SELECT
        date_format(`collections`.`created_at`,'%Y-%m-%d') AS date,
        MONTH(`collections`.`created_at`)as mes,
        YEAR(`collections`.`created_at`)as year,
        collections.branch_office_id AS branch_office_id,
        concat(`collections`.`branch_office_id`,date_format(`collections`.`created_at`,'%Y'),date_format(`collections`.`created_at`,'%m') * 1) AS clave,
        0 AS ticket_number,
        sum(collections.net_amount) AS abonados,
        0 AS net_amount,
        sum(collections.card_net_amount) AS transbank
    FROM collections
    LEFT JOIN cashiers
    ON collections.cashier_id = cashiers.cashier_id
    LEFT JOIN branch_offices
    ON collections.branch_office_id = branch_offices.branch_office_id
    WHERE
        cashiers.cashier_type_id = 3 AND
        collections.created_at >= '2022-12-31' AND
        branch_offices.principal <> 'ADMINISTRACION'
    GROUP BY
        collections.branch_office_id,
        collections.created_at;
    """
    abonados_neto = pd.read_sql(query, engine)
    return abonados_neto


def qry_ppto_dia():
    engine = connect_to_db()
    query = """
    SELECT 
    date,
    MONTH(date)as mes,
    YEAR(date)as year,
    branch_office_id,
    ppto 
    FROM 
    QRY_PPTO_DIA"""
    ppto = pd.read_sql(query, engine)
    return ppto 


def qry_branch_offices():
    engine = connect_to_db()
    query = "SELECT * FROM QRY_BRANCH_OFFICES WHERE status_id = 15"
    sucursales = pd.read_sql(query, engine)
    return sucursales 


def qry_sss():
    engine = connect_to_db()
    query = "SELECT * FROM QRY_SSS"
    sss = pd.read_sql(query, engine)
    return sss 

def generar_periodos(mes_actual):
   periodos = ['01-Enero','02-Febrero','03-Marzo','04-Abril','05-Mayo','06-Junio',
              '07-Julio','08-Agosto','09-Septiembre','10-Octubre','11-Noviembre','12-Diciembre']
   periodos[mes_actual-1] = 'Acumulado'
   return periodos   

def main(authenticated=True):    
    if not authenticated:
        raise Exception("No autenticado, Necesitas autenticarte primero")
        #return
        #st.error("Necesitas autenticarte primero")
        #return
    else:
        
        df_recaudacion = tp_recaudacion_neto()
        df_abonados = tp_abonados_neto()
        df_sucursales = qry_branch_offices()
        df_ppto = qry_ppto_dia()
        df_sss = qry_sss()
        
        # Establecer "date" como índice en ambos dataframes
        df_recaudacion.set_index("date", inplace=True)
        df_abonados.set_index("date", inplace=True)

       # Concatenate dataframes using 'date' as index
        df_concatenado = pd.concat([df_recaudacion, df_abonados])
        # Reset index to include 'date' column in the dataframe
        df_concatenado.reset_index(inplace=True)
        
        
        #st.dataframe(df_concatenado)
        
        ### INGRESOS ACTUAL 2024
        df_ingresos_2024 = df_concatenado[(df_concatenado["year"] == 2024)]
        columns_ingresos = ["date", "year", "mes", "branch_office_id" , "ticket_number" , "net_amount" , "transbank" , "abonados" ]
        df_ingresos_act = df_ingresos_2024[columns_ingresos]
    
        df_ingresos_act = df_ingresos_act.rename(columns={"ticket_number": "ticket_number_Act", 
                                            "net_amount" : "Venta_Neta_Act" ,
                                            "transbank": "Transbank_Act",
                                            "abonados" : "Abonados_Act"})        
        st.dataframe(df_ingresos_act)
        
        
        ### INGRESOS ANTERIOR 2023
        df_ingresos_2023 = df_concatenado[(df_concatenado["year"] == 2023)]
        columns_ingresos = ["date", "year", "mes", "branch_office_id" , "ticket_number" , "net_amount" , "transbank" , "abonados" ]
        df_ingresos_ant = df_ingresos_2023[columns_ingresos]
    
        df_ingresos_ant = df_ingresos_ant.rename(columns={"ticket_number": "ticket_number_Ant", 
                                            "net_amount" : "Venta_Neta_Ant" ,
                                            "transbank": "Transbank_Ant",
                                            "abonados" : "Abonados_Ant"})        
        st.dataframe(df_ingresos_ant)
        
        
        
        
if __name__ == "__main__":
    main()    

        
        

