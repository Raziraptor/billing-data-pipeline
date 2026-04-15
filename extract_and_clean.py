import pandas as pd
from sqlalchemy import create_engine
import datetime

def connect_to_sql():
    """
    Simula la conexión a la base de datos SQL Server de facturación.
    """
    # En un entorno real, las credenciales estarían en variables de entorno por seguridad.
    db_connection_str = 'mssql+pyodbc://usuario:contraseña@servidor/FactuprontoDB?driver=SQL+Server'
    engine = create_engine(db_connection_str)
    return engine

def extract_billing_data(engine):
    """
    Extrae los datos masivos de facturación del último mes.
    """
    query = """
    SELECT 
        InvoiceID, 
        ClientID, 
        IssueDate, 
        TotalAmount, 
        TaxStatus,
        ClientRFC,
        ClientName
    FROM Invoices
    WHERE IssueDate >= DATEADD(month, -1, GETDATE())
    """
    # Cargamos los datos en un DataFrame de Pandas para su manipulación
    df = pd.read_sql(query, engine)
    print(f"Extraídos {len(df)} registros de la base de datos.")
    return df

def transform_for_palantir(df):
    """
    Limpia y anonimiza los datos preparándolos para la ingesta en Palantir Foundry.
    """
    # 1. Limpieza de datos nulos
    df = df.dropna(subset=['TotalAmount', 'ClientID'])
    
    # 2. Anonimización (Data Obfuscation) - Crucial para seguridad
    # Ocultamos el nombre real del cliente por políticas de privacidad
    df['ClientName'] = 'CONFIDENTIAL'
    
    # 3. Estandarización de fechas al formato ISO (Requerido por Palantir)
    df['IssueDate'] = pd.to_datetime(df['IssueDate']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    return df

def export_to_parquet(df, filename="palantir_ingestion_payload.parquet"):
    """
    Exporta los datos a formato Parquet, el estándar de compresión para macrodatos.
    """
    df.to_parquet(filename, engine='pyarrow')
    print(f"Archivo {filename} generado exitosamente y listo para ingesta en Palantir.")

if __name__ == "__main__":
    print("Iniciando pipeline de integración de datos...")
    
    # engine = connect_to_sql() # Comentado para que el script no falle al no tener un SQL real
    
    # Simularemos los datos para que tu código en GitHub sea ejecutable y demostrable
    simulated_data = {
        'InvoiceID': ['F-1001', 'F-1002', 'F-1003'],
        'ClientID': [450, 451, 452],
        'IssueDate': ['2023-10-01 14:30:00', '2023-10-02 09:15:00', '2023-10-02 16:45:00'],
        'TotalAmount': [15500.50, 3200.00, 45000.75],
        'TaxStatus': ['Paid', 'Pending', 'Paid'],
        'ClientRFC': ['XAXX010101000', 'XEXX010101000', 'GZPX890101ABC'],
        'ClientName': ['Empresa A', 'Empresa B', 'Grupo Garza']
    }
    raw_df = pd.DataFrame(simulated_data)
    
    clean_df = transform_for_palantir(raw_df)
    export_to_parquet(clean_df)
    print("Pipeline finalizado.")
