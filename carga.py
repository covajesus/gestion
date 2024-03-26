import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# Conexión a la base de datos MySQL
def connect_to_db():
    db_config = {
        'host': '103.72.78.28',
        'user': 'jysparki_jis',
        'password': 'Jis2020!',
        'database': 'jysparki_jis'}
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    return engine

def update_kpi_ingresos_acumulado_act(cargar=True):
    engine = connect_to_db()
    # Utiliza engine.connect() para obtener una conexión
    with engine.connect() as connection:        
        if cargar:
            # Consulta de eliminación
            delete_query = text("DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2024' and metrica = 'ingresos' AND periodo != 'Acumulado'")
            # Ejecutar la consulta de eliminación
            connection.execute(delete_query)      
            # Consulta de inserción
            insert_query = text("""
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
      SELECT
			'Acumulado' as Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office,
            QRY_IND_SSS.`value`,
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number,
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados,
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount,
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta,
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos,
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
        FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
        WHERE
	        QRY_BRANCH_OFFICES.status_id = 15 AND
	        DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) < (DAY(CURDATE())) AND
	        MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate()))) AND
	        YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())
        GROUP BY
	        DM_PERIODO.Periodo,
	        DM_PERIODO.period,
	        DM_PERIODO.`Año`,
	        QRY_BRANCH_OFFICES.branch_office
        ORDER BY
	        QRY_INGRESOS_TOTALES_PBI.date ASC""")
            st.write(insert_query)
            # Ejecutar la consulta de inserción
            connection.execute(insert_query)           
            st.write("Ingresos Actual Acumulado, Cargados con exito.")
            



def update_kpi_ingresos_acumulado_ant(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:
        if cargar:
            delete_query = text("DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2023' AND Periodo = 'Acumulado' and metrica = 'ingresos';")
            connection.execute(delete_query)
            insert_query = text("""
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT  
            'Acumulado' as Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office, 
            QRY_IND_SSS.`value`, 
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number, 
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados, 
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount, 
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,	
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta, 
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos, 
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS, 
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
            FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
            WHERE	
	        QRY_BRANCH_OFFICES.status_id = 15 AND
	        DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) < (DAY(CURDATE())) AND
	        MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate()))) AND
	        YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())-1
        GROUP BY 
	        DM_PERIODO.Periodo,
	        DM_PERIODO.period,
	        DM_PERIODO.`Año`,
	        QRY_BRANCH_OFFICES.branch_office
        ORDER BY
	        QRY_INGRESOS_TOTALES_PBI.date ASC""")
            connection.execute(insert_query)
            st.write("Ingresos Anterior Acumulado, Cargados con exito.")

def update_kpi_ingresos_acumulado_ppto(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:
        if cargar:
            set_locale_query = text("SET lc_time_names = 'es_ES'")
            delete_query = text("DELETE FROM KPI_INGRESOS_IMG_MES WHERE metrica = 'ppto' AND periodo = 'Acumulado';")
            connection.execute(delete_query)
            insert_query = text("""
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT
            'Acumulado' AS periodo,
            CONCAT(YEAR(QRY_PPTO_DIA.date), '-', LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0')) AS period,
            YEAR(QRY_PPTO_DIA.date) AS año,
            QRY_BRANCH_OFFICES.branch_office AS branch_office,
            '0' AS `value`,
            '0' AS `ticket_number`,
            '0' AS `abonados`,
            '0' AS `net_amount`,
            '0' AS transbank,
            '0' AS Venta_Neta,
	    	'0' AS Ingresos,
            SUM(QRY_PPTO_DIA.ppto) AS ppto,
            '0' AS Venta_SSS,
            '0' AS Ingresos_SSS,
            'ppto' AS metrica
            FROM QRY_PPTO_DIA
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_PPTO_DIA.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            WHERE 
            DAY(QRY_PPTO_DIA.date) < DAY(CURDATE()) AND
             MONTH(QRY_PPTO_DIA.date) = MONTH(CURDATE()) AND
            YEAR(QRY_PPTO_DIA.date) = YEAR(CURDATE())
            GROUP BY
            QRY_BRANCH_OFFICES.branch_office""")
            connection.execute(insert_query)
            st.write("Ingresos Presupuesto Acumulado, Cargados con exito.")
                     
def update_kpi_recaudacion_dia(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:
        if cargar:
            # Consulta de eliminación
            delete_query = text("DELETE FROM KPI_DTES_RECAUDACION_DIA")
            # Ejecutar la consulta de eliminación
            connection.execute(delete_query) 
            insert_query = text("""
                INSERT INTO KPI_DTES_RECAUDACION_DIA (Fecha, branch_office_id, recaudacion)
                SELECT
                    DATE_FORMAT(collections.created_at, '%Y-%m-%d') AS Fecha,
                    collections.branch_office_id AS branch_office_id,
                    SUM(collections.gross_amount) AS recaudacion
                FROM collections
                LEFT JOIN cashiers ON collections.cashier_id = cashiers.cashier_id
                LEFT JOIN branch_offices ON collections.branch_office_id = branch_offices.branch_office_id
                WHERE
                    collections.special_cashier = 0 AND
                    cashiers.cashier_type_id <> 3 AND
                    branch_offices.status_id = 15 AND
                    collections.created_at < CURDATE() - INTERVAL 1 DAY AND
                    YEAR(collections.created_at) = YEAR(CURDATE())
                GROUP BY
                    DATE_FORMAT(collections.created_at, '%Y-%m-%d'),
                    collections.branch_office_id
            """)
            connection.execute(insert_query)
            st.write("Recaudación diaria cargada con éxito.")
            
def update_kpi_depositos_dia(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:
        if cargar:
            # Consulta de eliminación
            delete_query = text("DELETE FROM KPI_DTES_DEPOSITOS_DIA")
            # Ejecutar la consulta de eliminación
            connection.execute(delete_query) 
            insert_query = text("""
                INSERT INTO KPI_DTES_DEPOSITOS_DIA (Fecha, branch_office_id, deposito)
                SELECT
                    DATE_FORMAT(deposits.collection_date, '%Y-%m-%d') AS Fecha,
                    deposits.branch_office_id AS branch_office_id,
                    SUM(deposits.deposit_amount) AS deposito
                FROM deposits
                LEFT JOIN statuses ON deposits.status_id = statuses.status_id
                LEFT JOIN QRY_BRANCH_OFFICES ON deposits.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
                WHERE
                    deposits.collection_date < CURDATE() AND
                    YEAR(deposits.collection_date) = YEAR(CURDATE()) AND
                    QRY_BRANCH_OFFICES.status_id = 15
                GROUP BY
                    DATE_FORMAT(deposits.collection_date, '%Y-%m-%d'),
                    deposits.branch_office_id
            """)
            connection.execute(insert_query)
            st.write("Depósitos diarios cargados con éxito.")            
            
def update_abonados(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:
        if cargar:
            # Consulta de eliminación
            delete_query = text("DELETE FROM TP_ABONADOS")
            #Ejecutar la consulta de eliminación
            connection.execute(delete_query) 
            insert_query = text(""" INSERT INTO TP_ABONADOS
            SELECT
	            dtes.dte_id AS dte_id,
	            DATE_FORMAT(dtes.created_at,"%Y-%m-%d") AS date,
	            dtes.rut AS rut,
                UPPER(users.`names`) AS cliente,
                CONCAT_WS(" - ",dtes.rut,UPPER(users.`names`))as razon_social,
	            dtes.folio AS folio,
	            dtes.branch_office_id AS branch_office_id,
	            dtes.dte_type_id AS dte_type_id,
	            dtes.status_id AS status_id,
	            dtes.amount AS amount,
	            dtes.period AS period,
	            dtes.`comment` AS `comment`,
	            statuses.`status` AS `status`,
	            dtes.chip_id AS chip_id
            FROM dtes
                LEFT JOIN customers ON dtes.rut = customers.rut
                LEFT JOIN users ON customers.rut = users.rut
                LEFT JOIN statuses ON dtes.status_id = statuses.status_id
            WHERE
	            dtes.rut <> '66666666-6' AND
	            dtes.dte_version_id = 1 AND
	            dtes.status_id > 17 AND
	            dtes.status_id < 24 AND
	            users.rol_id = 14 AND
	        YEAR(dtes.created_at) = (YEAR(curdate()))""")
            connection.execute(insert_query)
            st.write("Abonados Mensuales, cargados con éxito.")
                                  
def update_kpi_ingresos_mensual_act(cargar=False):
    engine = connect_to_db()
    # Utiliza engine.connect() para obtener una conexión
    with engine.connect() as connection:        
        if cargar:
            # Consulta de eliminación
            delete_query = text("DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2024' and metrica = 'ingresos' AND periodo != 'Acumulado'")
            # Ejecutar la consulta de eliminación
            connection.execute(delete_query)      
            # Consulta de inserción
            insert_query = text("""              
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT
            DM_PERIODO.Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office, 
            QRY_IND_SSS.`value`, 
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number, 
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados, 
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount, 
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,	
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta, 
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos, 
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS, 
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
            FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
            WHERE	
                QRY_BRANCH_OFFICES.status_id = 15 AND
                #DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) <= (DAY(CURDATE())-1) AND
                #MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate())-1)) AND
                YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())
            GROUP BY 
                DM_PERIODO.Periodo,
                DM_PERIODO.period,
                DM_PERIODO.`Año`,
                QRY_BRANCH_OFFICES.branch_office
            ORDER BY
                QRY_INGRESOS_TOTALES_PBI.date ASC""") 
            # Ejecutar la consulta de inserción
            connection.execute(insert_query)           
            st.write("Ingresos Actual Mensual, Cargados con exito.")
            
def update_kpi_ingresos_mensual_ant(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:        
        if cargar:
            # Consulta de eliminación
            delete_query = text("DELETE FROM KPI_INGRESOS_IMG_MES WHERE año = '2023' and metrica = 'ingresos' AND periodo != 'Acumulado'")
            # Ejecutar la consulta de eliminación
            connection.execute(delete_query)      
            # Consulta de inserción
            insert_query = text("""              
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto , Venta_SSS, Ingresos_SSS, metrica)
            SELECT
            DM_PERIODO.Periodo,
            DM_PERIODO.period,
            DM_PERIODO.`Año`,
            QRY_BRANCH_OFFICES.branch_office, 
            QRY_IND_SSS.`value`, 
            SUM(QRY_INGRESOS_TOTALES_PBI.ticket_number)AS ticket_number, 
            SUM(QRY_INGRESOS_TOTALES_PBI.abonados) AS abonados, 
            SUM(QRY_INGRESOS_TOTALES_PBI.net_amount) AS net_amount, 
            SUM(QRY_INGRESOS_TOTALES_PBI.transbank) AS transbank,	
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank)) AS Venta_Neta, 
            SUM((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados)) AS Ingresos, 
            '0' as ppto,
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank) * (QRY_IND_SSS.`value`))) AS Venta_SSS, 
            SUM(((QRY_INGRESOS_TOTALES_PBI.net_amount + QRY_INGRESOS_TOTALES_PBI.transbank + QRY_INGRESOS_TOTALES_PBI.abonados) * (QRY_IND_SSS.`value`))) AS Ingresos_SSS,
            'ingresos' as metrica
            FROM QRY_INGRESOS_TOTALES_PBI
            LEFT JOIN QRY_BRANCH_OFFICES
            ON QRY_INGRESOS_TOTALES_PBI.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            LEFT JOIN QRY_IND_SSS
            ON QRY_INGRESOS_TOTALES_PBI.clave = QRY_IND_SSS.clave
            LEFT JOIN DM_PERIODO
            ON QRY_INGRESOS_TOTALES_PBI.date = DM_PERIODO.Fecha
            WHERE	
                QRY_BRANCH_OFFICES.status_id = 15 AND
                #DAY(`QRY_INGRESOS_TOTALES_PBI`.`date`) <= (DAY(CURDATE())-1) AND
                #MONTH(`QRY_INGRESOS_TOTALES_PBI`.`date`) = ((MONTH(curdate())-1)) AND
                YEAR(`QRY_INGRESOS_TOTALES_PBI`.`date`) = YEAR(curdate())-1                
            GROUP BY 
                DM_PERIODO.Periodo,
                DM_PERIODO.period,
                DM_PERIODO.`Año`,
                QRY_BRANCH_OFFICES.branch_office
            ORDER BY
                QRY_INGRESOS_TOTALES_PBI.date ASC""")
            # Ejecutar la consulta de inserción
            connection.execute(insert_query)       
            st.write("Ingresos Anterior Mensual, Cargados con exito.")
            
def update_kpi_ingresos_mensual_ppto(cargar=False):
    engine = connect_to_db()
    with engine.connect() as connection:        
        if cargar:
            # Configurar el locale para lc_time_names
            set_locale_query = text("SET lc_time_names = 'es_ES'")
            connection.execute(set_locale_query)
            # Consulta de eliminación
            delete_query = text("DELETE FROM KPI_INGRESOS_IMG_MES WHERE metrica = 'ppto' and Period != 'Acumulado'")
            # Ejecutar la consulta de eliminación
            connection.execute(delete_query)
            # Consulta de inserción
            insert_query = text("""            
            INSERT INTO KPI_INGRESOS_IMG_MES (periodo, period, año, branch_office, `value`, ticket_number, abonados, net_amount , transbank, Venta_Neta , Ingresos, ppto, Venta_SSS, Ingresos_SSS, metrica)
            SELECT 	
            CONCAT(LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0'),'-',UPPER(SUBSTRING(MONTHNAME(QRY_PPTO_DIA.date), 1, 1)), 
                   LOWER(SUBSTRING(MONTHNAME(QRY_PPTO_DIA.date), 2))) AS periodo,
            CONCAT(YEAR(QRY_PPTO_DIA.date), '-', LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0')) AS period,
            YEAR(QRY_PPTO_DIA.date) as año,    
            QRY_BRANCH_OFFICES.branch_office as branch_office,
                '0' as `value`,
                '0' as `ticket_number`,
                '0' as `abonados`,
                '0' as `net_amount`,
                '0' as transbank,
                '0' as Venta_Neta,	
                '0' as Ingresos,
            SUM(QRY_PPTO_DIA.ppto) as ppto,
                '0' as Venta_SSS,		
                '0' as Ingresos_SSS,
                'ppto' as metrica
            FROM QRY_PPTO_DIA 
            LEFT JOIN QRY_BRANCH_OFFICES
                ON QRY_PPTO_DIA.branch_office_id = QRY_BRANCH_OFFICES.branch_office_id
            WHERE 
                #DAY(QRY_PPTO_DIA.date) < (DAY(CURDATE())) AND
                #MONTH(QRY_PPTO_DIA.date) = ((MONTH(curdate()))) AND
                YEAR(QRY_PPTO_DIA.date) = YEAR(curdate())                
            GROUP BY
                CONCAT(YEAR(QRY_PPTO_DIA.date), '-', LPAD(MONTH(QRY_PPTO_DIA.date), 2, '0')),
                QRY_BRANCH_OFFICES.branch_office""")
            # Ejecutar la consulta de inserción
            connection.execute(insert_query)       
            st.write("Ingresos Presupuesto Mensual, Cargados con exito.")
           
def cargar_datos(opcion1, opcion2, opcion3):
    if opcion1 == "Informe de ventas":
        if opcion2 == "Acumulado":
            if opcion3 == "Actual":
                update_kpi_ingresos_acumulado_act(cargar=True)
            elif opcion3 == "Año Anterior":
                update_kpi_ingresos_acumulado_ant(cargar=True)
            elif opcion3 == "Ppto":
                update_kpi_ingresos_acumulado_ppto(cargar=True)
        elif opcion2 == "Mensual":            
            if opcion3 == "Actual":
                update_kpi_ingresos_mensual_act(cargar=True)
            elif opcion3 == "Año Anterior":
                update_kpi_ingresos_mensual_ant(cargar=True)
            elif opcion3 == "Ppto":
                update_kpi_ingresos_mensual_ppto(cargar=True)
    elif opcion1 == "Abonados":
        update_abonados(cargar=True)    
    elif opcion1 == "Depositos":
        if opcion2 == "Recaudacion":
            update_kpi_recaudacion_dia(cargar=True)
        elif opcion2 == "Depositos":
            update_kpi_depositos_dia(cargar=True)
    else:
        st.write(f"No se encontraron datos para {opcion1} - {opcion2} - {opcion3}")

def main(authenticated=False):
    if not authenticated:
        #st.error("Necesitas autenticarte primero")
        #st.error("Necesitas autenticarte")
        #st.stop()
        raise Exception("No autenticado, Necesitas autenticarte primero")
        #return
        #st.error("Necesitas autenticarte primero")
        #return    
    else:
        st.title("Carga de datos")
        opcion1 = st.selectbox("Selecciona una opción", ["Informe de ventas", "Abonados", "Depositos"])        
        if opcion1 == "Informe de ventas":
            opcion2 = st.selectbox("Selecciona una opción", ["Acumulado", "Mensual"])
            if opcion2 == "Acumulado":
                opcion3 = st.selectbox("Selecciona una opción", ["Actual", "Año Anterior", "Ppto"])
                if st.button("Carga", key="carga_acumulado"):
                    cargar_datos(opcion1, opcion2, opcion3)
            elif opcion2 == "Mensual":
                opcion3 = st.selectbox("Selecciona una opción", ["Actual", "Año Anterior", "Ppto"])
                if st.button("Carga", key="carga_mensual"):
                    cargar_datos(opcion1, opcion2, opcion3)
        elif opcion1 == "Depositos":
            opcion2 = st.selectbox("Selecciona una opción", ["Recaudacion", "Depositos"])
            if opcion2 == "Recaudacion":
                if st.button("Carga", key="carga_recaudacion"):
                    cargar_datos(opcion1, opcion2, None)
            elif opcion2 == "Depositos":
                if st.button("Carga", key="carga_depositos"):
                    cargar_datos(opcion1, opcion2, None)
        elif opcion1 == "Abonados":
            if st.button("Carga", key="carga_abonados"):
                cargar_datos(opcion1, None, None)
                                  
    
if __name__ == "__main__":
    main()   
