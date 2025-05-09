import pandas as pd
import numpy as np
import os
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from slack_alerts_pro import start_alert, success_alert, failure_alert
from datetime import datetime


#src_sql_server_conn_id = 'admin_migration_pagatae_origin'
#conexion origen final
src_sql_server_conn_id = 'admin_origin_final_pagatae'



#dst_sql_server_conn_id = 'admin_migration_pagatae_destiny'
#conexion destino final
dst_sql_server_conn_id = 'admin_destiny_final_pagatae' 


# Función de imputación de fechas
def impute_dates(df):
    datetime_columns = ['generator_date']  # Agrega aquí las columnas de tipo datetime
    for col in datetime_columns:
        if col in df.columns:
            # Reemplazar fechas nulas con una fecha válida (por ejemplo, 1970-01-01)
            df[col] = df[col].apply(lambda x: pd.Timestamp('1970-01-01 00:00:00.000') if pd.isna(x) else x)
            
            # Asegurarse de que todas las fechas estén dentro del rango válido para SQL Server
            min_date = pd.Timestamp('1753-01-01 00:00:00.000')
            max_date = pd.Timestamp('9999-12-31 23:59:59.999')
            df[col] = df[col].apply(lambda x: max(min(x, max_date), min_date))

            # Convertir a string en formato compatible con SQL Server (YYYY-MM-DD HH:MM:SS.sss)
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]  # Mantiene solo milisegundos (SQL Server acepta hasta 3)
    
    return df
    

    
# Función para obtener las entidades por ID
def get_entities_by_ids(entity_ids, hook):
    if not entity_ids:
        return pd.DataFrame()
    
    ids_str = ', '.join(map(str, entity_ids))
    query = f"SELECT ID, parentID, FK_bolsas, name, website, phonenumber, comments, rfc, direccion, image, deleted, canAddEntities, canAddUsers, balance, comision, genTree, numeroTienda, canAddIVR, generator_entities, generator_users, generator_date, FK_vendedor FROM entities WHERE ID IN ({ids_str})"
    return hook.get_pandas_df(query)

# Función para obtener los hijos de las entidades
def get_child_entities_by_ids(parent_ids, hook):
    if not parent_ids:
        return pd.DataFrame()
    
    ids_str = ', '.join(map(str, parent_ids))
    query = f"SELECT ID, parentID, FK_bolsas, name, website, phonenumber, comments, rfc, direccion, image, deleted, canAddEntities, canAddUsers, balance, comision, genTree, numeroTienda, canAddIVR, generator_entities, generator_users, generator_date, FK_vendedor FROM entities WHERE deleted = 0 AND parentID IN ({ids_str})"
    return hook.get_pandas_df(query)

# Función para actualizar las claves foráneas y obtener los cambios
def update_foreign_keys_and_get_changes(df, fk_column, id_mapping):
    updated_df = df.copy()
    updated_df[fk_column] = df[fk_column].map(id_mapping).fillna(df[fk_column]).astype(int)
    changed_rows = updated_df[updated_df[fk_column] != df[fk_column]]
    return changed_rows





# Función para insertar el DataFrame en la base de datos de destino
def insert_dataframe(df, table_name, dst_hook):
    if df.empty:
        print("No hay datos para transferir.")
        return

    column_names = ', '.join(df.columns)
    
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Habilitar IDENTITY_INSERT
            cursor.execute(f"SET IDENTITY_INSERT {table_name} ON;")
            conn.commit()

            # Crear un diccionario para llevar un control de los nombres renombrados
            name_renaming = {}

            for _, row in df.iterrows():
                # Verificar si ya existe la combinación (parentID, name) en la base de datos destino
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE parentID = '{row['parentID']}' AND name = '{row['name']}'"
                cursor.execute(check_query)
                result = cursor.fetchone()
                
                if result[0] > 0:
                    # Si existe, renombrar el campo 'name' añadiendo un sufijo único
                    new_name = f"{row['name']}_{str(np.random.randint(10, 99))}"
                    row['name'] = new_name
                    # Guardar el cambio para tenerlo como referencia
                    name_renaming[row['ID']] = new_name
                    print(f"El nombre {row['name']} ya existe. Se ha renombrado a {new_name}.")
                else:
                    print(f"El nombre {row['name']} es único. No se requiere renombrar.")

                # Formatear los valores de la fila después de renombrar el campo 'name'
                values = ', '.join([f"'{value}'" if isinstance(value, (str, pd.Timestamp)) else ('NULL' if pd.isna(value) else str(value)) 
                                    for value in row])

                # Insertar el registro (ya con nombre renombrado si es necesario)
                insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values});"
                print('Ejecutando consulta:', insert_query)
                cursor.execute(insert_query)

            conn.commit()
            
            # Deshabilitar IDENTITY_INSERT
            cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF;")
            conn.commit()

    print("Transferencia completada con éxito.")





def insert_users_dataframe(df, table_name, dst_hook):
    if df.empty:
        print("No hay datos para transferir.")
        return

    column_names = ', '.join(df.columns)
    
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Habilitar IDENTITY_INSERT para la tabla de usuarios
            cursor.execute(f"SET IDENTITY_INSERT {table_name} ON;")
            conn.commit()

            # Crear un diccionario para llevar un control de los nombres renombrados
            name_renaming = {}

            for _, row in df.iterrows():
                # Verificar si ya existe un usuario con el mismo nombre en la base de datos destino
                check_query = f"SELECT COUNT(*) FROM {table_name} WHERE name = '{row['name']}'"
                cursor.execute(check_query)
                result = cursor.fetchone()
                
                if result[0] > 0:
                    # Si existe, renombrar el campo 'name' añadiendo un sufijo único
                    new_name = f"{row['name']}_{str(np.random.randint(10, 99))}"
                    row['name'] = new_name
                    # Guardar el cambio para tenerlo como referencia
                    name_renaming[row['ID']] = new_name
                    print(f"El nombre {row['name']} ya existe. Se ha renombrado a {new_name}.")
                else:
                    print(f"El nombre {row['name']} es único. No se requiere renombrar.")

                # Formatear los valores de la fila después de renombrar el campo 'name'
                
                values = ', '.join(
                    [f"'{value}'" if isinstance(value, (str, pd.Timestamp)) 
                     else ('NULL' if pd.isna(value) else str(value)) 
                     for value in row]
                )
                

              
                # Construir la consulta de inserción
                insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values});"
                print('Ejecutando consulta:', insert_query)
                cursor.execute(insert_query)

            conn.commit()
            
            # Deshabilitar IDENTITY_INSERT
            cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF;")
            conn.commit()

    print("Inserción de usuarios completada con éxito.")





#insertar entities carrieers
def insert_carriers_dataframe(df, table_name, dst_hook):
    """
    Inserta un DataFrame en la tabla `entities_carriers` de SQL Server.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a insertar.
        table_name (str): Nombre de la tabla de destino.
        dst_hook (MsSqlHook): Hook de conexión a la base de datos de destino.
    """
    if df.empty:
        print("No hay datos para transferir.")
        return


    # Obtener los nombres de las columnas
    column_names = ', '.join(df.columns)

    # Insertar los datos en la base de datos
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for _, row in df.iterrows():
                # Formatear los valores de la fila
                values = ', '.join(
                    [f"'{value}'" if isinstance(value, (str, pd.Timestamp)) 
                     else ('NULL' if pd.isna(value) else str(value)) 
                     for value in row]
                )

                # Construir la consulta de inserción
                insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values});"
                print('Ejecutando consulta:', insert_query)
                cursor.execute(insert_query)

            conn.commit()

    print("Inserción de entities carriers completada con éxito.")





def insert_clientesFacturacion_dataframe(df, table_name, dst_hook):
    """
    Inserta un DataFrame en la tabla `clientesFacturacion` de SQL Server.

    Parámetros:
        df (pd.DataFrame): DataFrame que contiene los datos a insertar.
        table_name (str): Nombre de la tabla de destino.
        dst_hook (MsSqlHook): Hook de conexión a la base de datos de destino.
    """
    if df.empty:
        print("No hay datos para transferir.")
        return

    column_names = ', '.join(df.columns)
    
    with dst_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            # Habilitar IDENTITY_INSERT
            cursor.execute(f"SET IDENTITY_INSERT {table_name} ON;")
            conn.commit()

            for _, row in df.iterrows():
                # Formatear los valores de la fila
                values = []
                for value in row:
                    if pd.isna(value):
                        values.append('NULL')
                    elif isinstance(value, (str, pd.Timestamp)):
                        values.append(f"'{value}'")
                    elif isinstance(value, bytes):  # Manejar datos binarios (columna cedula)
                        values.append(f"0x{value.hex()}")  # Convertir bytes a formato hexadecimal
                    elif isinstance(value, bool):  # Manejar columnas booleanas (bit)
                        values.append('1' if value else '0')
                    else:
                        values.append(str(value))

                values_str = ', '.join(values)

                # Construir la consulta de inserción
                insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({values_str});"
                print('Ejecutando consulta:', insert_query)
                cursor.execute(insert_query)

            conn.commit()
            
            # Deshabilitar IDENTITY_INSERT
            cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF;")
            conn.commit()

    print("Inserción en clientesFacturacion completada con éxito.")




# Ruta al archivo que contiene los IDs iniciales
INITIAL_STRUCTURE_FILE_PATH = "/root/airflow/dags/initial_structure.txt"



def read_initial_structure(file_path):
    """
    Lee los IDs iniciales desde un archivo externo.

    Parámetros:
        file_path (str): Ruta al archivo que contiene los IDs iniciales.

    Retorna:
        list: Lista de IDs iniciales.
    """
    try:
        with open(file_path, "r") as file:
            content = file.read().strip()
            if not content:
                raise ValueError("El archivo está vacío.")
            
            # Convertir los IDs a una lista de enteros
            initial_structure = [int(id.strip()) for id in content.split(",")]
            return initial_structure
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo {file_path} no existe.")
    except Exception as e:
        raise ValueError(f"Error al leer el archivo: {e}")
        
'''
def save_id_mapping_to_csv(id_mapping, file_path):

    try:
        # Convertir el diccionario a un DataFrame
        df = pd.DataFrame(list(id_mapping.items()), columns=["original_id", "new_id"])
        
        # Guardar el DataFrame en un archivo CSV
        df.to_csv(file_path, index=False)
        print(f"id_mapping guardado en {file_path}")
    except Exception as e:
        print(f"Error al guardar id_mapping en CSV: {e}")
        raise
'''

def save_id_mapping_to_csv(id_mapping, file_path, mapping_name = 'id_mapping'):

    try:
        df = pd.DataFrame(list(id_mapping.items()), columns=["original_id", "new_id"])
        full_path = f'{mapping_name}_{file_path}.csv'        
        df.to_csv(full_path, index=False)
        print(f'Mapeo {mapping_name} guardado en {full_path}')
        return full_path
    except Exception as e:
        print(f"Error al guardar id_mapping en CSV: {e}")
        raise




# Función principal de procesamiento de datos
def process_entities(**kwargs):
    # Obtener los hooks para las bases de datos
    src_hook = MsSqlHook(mssql_conn_id=src_sql_server_conn_id)  # Conexión a la base de datos de origen
    dst_hook = MsSqlHook(mssql_conn_id=dst_sql_server_conn_id)  # Conexión a la base de datos de destino
    
    # Leer los IDs iniciales desde el archivo externo
    try:
        initial_structure = read_initial_structure(INITIAL_STRUCTURE_FILE_PATH)
        #initial_structure = read_initial_structure(initial_structure.txt)
        print(f"IDs iniciales leídos desde el archivo: {initial_structure}")
    except Exception as e:
        print(f"Error al leer los IDs iniciales: {e}")
        raise
    
    # Obtener el último ID de las tablas desde la base de datos de origen
    last_id_query = "SELECT MAX(ID) AS last_id FROM entities"
    last_users_id_query = "SELECT MAX(ID) AS last_user_id FROM users"
    last_eRFC_id_query = 'SELECT MAX(ID) AS last_entities_RFC_id FROM clientesFacturacion'
    
    
    last_id = dst_hook.get_pandas_df(last_id_query).iloc[0]['last_id'] or 0
    print('El ultimo ID de entities destino es:', last_id)
    
    last_users_id = dst_hook.get_pandas_df(last_users_id_query).iloc[0]['last_user_id'] or 0
    print('El ultimo ID de los usuarios destino es:', last_users_id)
    
    last_eRFC_id = dst_hook.get_pandas_df(last_eRFC_id_query).iloc[0]['last_entities_RFC_id'] or 0
    print('El ultimo ID de entities FRC destino es:', last_eRFC_id)

    # Establecer estructura inicial
    #initial_structure = [34]

    initial_structure = list(set(initial_structure))
    
    BATCH_SIZE = 10
    
    processed_ids = set()
    id_mapping = {}
    records_to_insert = []
    queue = initial_structure.copy()
    print(f'Inicio del proceso con IDs iniciales {queue}')
    
    while queue:
        current_batch = queue[:BATCH_SIZE]
        queue = queue[BATCH_SIZE:]
        print(f"Procesando batch: {current_batch}")

        current_batch = [i for i in current_batch if i not in processed_ids]
        if not current_batch:
            print("Todos los IDs en este batch ya fueron procesados. Pasando al siguiente batch.")
            continue
        
        entities_df = get_entities_by_ids(current_batch, src_hook)
        print(f"Resultados de la consulta para batch {current_batch}: {len(entities_df)} registros encontrados")
        
        if entities_df.empty:
            print("No se encontraron registros para estos IDs. Pasando al siguiente batch.")
            continue
        
        new_records = []
        new_parent_ids = []
        
        for _, row in entities_df.iterrows():
            current_id = row['ID']
            if current_id in processed_ids:
                continue
            
            processed_ids.add(current_id)
            last_id += 1
            new_id = last_id
            id_mapping[current_id] = new_id
            
            row['ID'] = new_id
            old_parent_id = row.get('parentID')
            old_genTree = row.get('genTree', '')
            
            if pd.notna(old_parent_id):
                updated_parent_id = id_mapping.get(old_parent_id, old_parent_id)
                row['parentID'] = updated_parent_id
                print(f"Actualizando parentID de {current_id} ({old_parent_id}) a {updated_parent_id}")
            else:
                row['parentID'] = None
                print(f"Entidad {current_id} es raíz, parentID se establece en None")
                
            if old_genTree:
                old_genTree_parts = old_genTree.split('.')
                new_genTree_parts = [str(id_mapping.get(int(part), part)) if part.isdigit() else part for part in old_genTree_parts]
                row['genTree'] = '.'.join(new_genTree_parts)
                print(f"genTree de {current_id} actualizado: {row['genTree']}")
            else:
                row['genTree'] = f"1.{new_id}"
                print(f"Entidad {current_id} sin genTree. Asignado: {row['genTree']}")
                
            new_records.append(row)
            new_parent_ids.append(current_id)
            
        if new_records:
            records_to_insert.extend(new_records)
            print(f"{len(new_records)} nuevos registros listos para inserción. Total acumulado: {len(records_to_insert)}")
            
        if new_parent_ids:
            child_df = get_child_entities_by_ids(new_parent_ids, src_hook)
            if not child_df.empty:
                child_ids = child_df['ID'].tolist()
                queue.extend(child_ids)
                print(f"Encontrados {len(child_ids)} hijos para los padres {new_parent_ids}. IDs agregados a la cola: {child_ids}.")
            else:
                print(f"No se encontraron hijos para los padres {new_parent_ids}.")
    
    print(f"Proceso finalizado. Total de registros a insertar: {len(records_to_insert)}")
    
    #guardamos (id mapping) en un archivo csv
    save_id_mapping_to_csv(id_mapping, "entities_pagatae_actual")
    
    
    
    # Aplicar imputación a las fechas
    entities_df = impute_dates(pd.DataFrame(records_to_insert))
    
    # Actualizar tablas relacionadas
    updated_users_df = pd.DataFrame()
    updated_carriers_df = pd.DataFrame()
    updated_entities_rfc_df = pd.DataFrame()
    users_to_insert = []
    
    if id_mapping:
        # Actualizamos la tabla de usuarios
        users_df = src_hook.get_pandas_df(f"SELECT * FROM users")
        updated_users_df = update_foreign_keys_and_get_changes(users_df, 'FK_entities', id_mapping)


        #Creamos mapeo de IDs para usuarios
        user_id_mapping = {}
        
        # Reindexamos los IDs de usuarios para registros nuevos
        for _, row in updated_users_df.iterrows():
            old_id = row['ID']
            last_users_id += 1  # Incrementar el last_users_id
            new_id = last_users_id
            user_id_mapping[old_id] = new_id
            row['ID'] = last_users_id
            row['old_id'] = old_id
            users_to_insert.append(row)
        

        #Guardamos el mapeo de usuarios
        users_mapping_path = save_id_mapping_to_csv(user_id_mapping, 'users_pagatae_actual')
        print(f'Mapeo de usuarios guardado en: {users_mapping_path}')
            
        
        # Actualizar tabla de carriers
        carriers_df = src_hook.get_pandas_df(f"SELECT * FROM entities_carriers")
        updated_carriers_df = update_foreign_keys_and_get_changes(carriers_df, 'FK_entities', id_mapping)
        
        
       
        
        
        
        
        # Actualizar tabla de clientesFacturacion
        #entities_rfc_df = src_hook.get_pandas_df(f"SELECT * FROM entities_RFC")
        clientesFacturacion_df = src_hook.get_pandas_df(f"SELECT * FROM clientesFacturacion")
        updated_clientesFacturacion_df = update_foreign_keys_and_get_changes(clientesFacturacion_df, 'FK_entities', id_mapping)
        updated_clientesFacturacion_df = update_foreign_keys_and_get_changes(updated_clientesFacturacion_df, 'FK_user', {row['old_id']: row['ID'] for row in users_to_insert})
        


        #Creamos mapeo de IDs para clientes facturacion
        cfacturacion_id_mapping = {}
        original_ids = clientesFacturacion_df['ID'].to_list()
        new_ids = range(last_eRFC_id + 1, last_eRFC_id + 1 + len(updated_clientesFacturacion_df))


        #Creamos el diccionarios de mapeo
        cfacturacion_id_mapping = dict(zip(original_ids, new_ids))

        #Guardamos el mapeo
        cfacturacion_path = save_id_mapping_to_csv(cfacturacion_id_mapping, 'cfacturacion_pagatae_actual')


        # Reindexar los IDs de clientesFacturacion
        updated_clientesFacturacion_df['ID'] = range(last_eRFC_id + 1, last_eRFC_id + 1 + len(updated_clientesFacturacion_df))
        last_eRFC_id += len(updated_clientesFacturacion_df)
    
    
    
    
    
    
    
    
    if users_to_insert:
        updated_users_df = pd.DataFrame(users_to_insert)
        updated_users_df = updated_users_df.drop('old_id', axis = 1)
        
    # Mostrar ejemplos de registros actualizados
    print('Registros de entities actualizados: \n\n')
    print(entities_df.shape)
    print(entities_df[['ID', 'parentID', 'genTree']].head(10))
    print('Columnas del Dataframe antes de la inserción \n:', entities_df.columns)
    print('Verificamos tipos de datos:\n\n', entities_df.dtypes)
    # Convertimos las columnas a tipos de dato válidos
    entities_df['ID'] = entities_df['ID'].astype(int)
    entities_df['parentID'] = entities_df['parentID'].astype(int)
    entities_df['FK_bolsas'] = entities_df['FK_bolsas'].astype(int)  # smallint
    entities_df['name'] = entities_df['name'].astype(str)  # nvarchar(90)
    entities_df['website'] = entities_df['website'].astype(str)  # nvarchar(64)
    entities_df['phonenumber'] = entities_df['phonenumber'].astype(str)  # nvarchar(64)
    entities_df['comments'] = entities_df['comments'].astype(str)  # ntext
    #entities_df['rfc'] = entities_df['rfc'].astype(str)  # nvarchar(13)
    entities_df['rfc'] = entities_df['rfc'].str.replace("'", "''")
    #entities_df['direccion'] = entities_df['direccion'].astype(str)  # nvarchar(192)
    entities_df['direccion'] = entities_df['direccion'].str.replace("'", "''")
    entities_df['image'] = entities_df['image'].astype(str)  # ntext
    entities_df['deleted'] = entities_df['deleted'].astype(int)  # smallint
    entities_df['canAddEntities'] = entities_df['canAddEntities'].astype(int)  # smallint
    entities_df['canAddUsers'] = entities_df['canAddUsers'].astype(int)  # smallint
    entities_df['balance'] = entities_df['balance'].astype(float)  # money (se convierte a float en Pandas)
    entities_df['comision'] = entities_df['comision'].astype(float)  # float
    entities_df['genTree'] = entities_df['genTree'].astype(str)  # nvarchar(64)
    entities_df['numeroTienda'] = entities_df['numeroTienda'].astype(str)  # nvarchar(16)
    entities_df['canAddIVR'] = entities_df['canAddIVR'].astype(int)  # smallint
    entities_df['generator_entities'] = entities_df['generator_entities'].astype('Int64')
    #entities_df['generator_entities'] = entities_df['generator_entities'].fillna(-1).astype(int)
    entities_df['generator_users'] = entities_df['generator_users'].astype(str)  # varchar(24)
    entities_df['generator_date'] = pd.to_datetime(entities_df['generator_date'])  # datetime
    entities_df['FK_vendedor'] = entities_df['FK_vendedor'].fillna(0).astype(int)
    
    #entities_df = impute_dates(pd.DataFrame(records_to_insert))
    #entities_df['generator_date'] = pd.to_datetime(entities_df['generator_date'])  # datetime
    
    # Verificamos los tipos de datos después de la conversión
    print("Tipos de datos después de la conversión:")
    print(entities_df.dtypes)
    print('\n\n\n', entities_df.isnull().sum())
    
    # Verificar que no haya valores NaN o NaT
    #ACEPTAMOS QUE EL CAMPO DE GENERATOR ENTITIES TENGA VALORES NULOS
    '''
    if entities_df.isna().any().any():
        raise ValueError("El DataFrame aún contiene valores NaN o NaT.")
    print('\n\n')
    '''
    
    
    print('Datos modificados: \n\n')
    print(entities_df[['ID', 'parentID', 'genTree', 'rfc']].head(10))
    print('\n\n\n')
    
    #Depuración adicional
    print('\n\nRegistros a insertar.')
    print(entities_df[['ID', 'parentID', 'name']].head(20))
    print('\n\n\n')
    
    
    print("Registros de usuarios actualizados de users:\n\n")
    print(updated_users_df.shape)
    print(updated_users_df[['ID', 'FK_entities']].head(20))
    print('Columnas del Dataframe antes de la inserción \n:', updated_users_df.columns)
    print('Verificamos tipos de datos:\n\n', updated_users_df.dtypes)
    #imputamos fechas
    #updated_users_df = impute_dates(updated_users_df)
    
    
    
    #Convertimos a tipos de datos valido
    updated_users_df['ID'] = updated_users_df['ID'].astype(int)
    updated_users_df['name'] = updated_users_df['name'].astype(str)
    updated_users_df['FK_entities'] = updated_users_df['FK_entities'].astype(int)
    updated_users_df['FK_schedules'] = updated_users_df['FK_schedules'].astype(int)
    updated_users_df['password_md5'] = updated_users_df['password_md5'].astype(str)
    updated_users_df['email'] = updated_users_df['email'].astype(str)
    updated_users_df['phone'] = updated_users_df['phone'].astype(str)
    updated_users_df['phone2'] = updated_users_df['phone2'].astype(str)
    updated_users_df['mobile'] = updated_users_df['mobile'].astype(str)
    updated_users_df['lastname'] = updated_users_df['lastname'].astype(str)
    #updated_users_df['firstname'] = updated_users_df['firstname'].str.replace("'", "''")
    updated_users_df['firstname'] = updated_users_df['firstname'].astype(str)
    updated_users_df['firstname'] = updated_users_df['firstname'].str.replace("'", "''")
    updated_users_df['location'] = updated_users_df['location'].astype(int)
    updated_users_df['active'] = updated_users_df['active'].astype(int)
    #updated_users_df['changepsw'] = updated_users_df['changepsw'].astype('int16')  # smallint
    updated_users_df['changepsw'] = updated_users_df['changepsw'].astype(int)
    #updated_users_df['deleted'] = updated_users_df['deleted'].astype('int16')  # smallint
    updated_users_df['deleted'] = updated_users_df['deleted'].astype(int)
    updated_users_df['usertype'] = updated_users_df['usertype'].astype(int)
    #updated_users_df['permisos'] = updated_users_df['permisos'].astype('int16')  # smallint
    updated_users_df['permisos'] = updated_users_df['permisos'].astype(int)  # smallint
    updated_users_df['wsIP'] = updated_users_df['wsIP'].astype(str)
    updated_users_df['licence'] = updated_users_df['licence'].astype(str)
    updated_users_df['prec_question'] = updated_users_df['prec_question'].astype(str)  # ntext
    updated_users_df['prec_answer'] = updated_users_df['prec_answer'].astype(str)  # ntext
    updated_users_df['numlicence'] = updated_users_df['numlicence'].astype(int)
    updated_users_df['generator_entities'] = updated_users_df['generator_entities'].astype(int)
    updated_users_df['generator_users'] = updated_users_df['generator_users'].astype(str)
    updated_users_df['generator_date'] = pd.to_datetime(updated_users_df['generator_date'])
    
    print("Tipos de datos después de la conversión:")
    print(updated_users_df.dtypes)
    print('\n\n\n', entities_df.isnull().sum())
    # Verificar que no haya valores NaN o NaT
    if updated_users_df.isna().any().any():
        raise ValueError("El DataFrame aún contiene valores NaN o NaT.")
    print('\n\n')
    
    
    
    updated_users_df = impute_dates(updated_users_df)
    #Depuración adicional
    print('Registros a insertar en users.')
    print(updated_users_df[['ID', 'FK_entities']].head(20))
    print('\n\n\n')
    print('Validacion de campo generator date')
    print(updated_users_df[['ID','generator_date']].tail(20))
    print('\n\n\n')
    
    
    
    print("Registros actualizados para entities carriers:\n\n")
    print(updated_carriers_df.shape)
    print(updated_carriers_df['FK_entities'].head(10))
    print('Columnas del Dataframe antes de la inserción \n:', updated_carriers_df.columns)
    print('Verificamos tipos de datos:\n\n', updated_carriers_df.dtypes)
    #convertimos a un tipo de dato valido
    updated_carriers_df['FK_entities'] = updated_carriers_df['FK_entities'].astype(int) 
    updated_carriers_df['FK_carriers'] = updated_carriers_df['FK_carriers'].astype(int)
    updated_carriers_df['comision'] = updated_carriers_df['comision'].astype(float)
    updated_carriers_df['ta'] = updated_carriers_df['ta'].astype(float)
    updated_carriers_df['costo'] = updated_carriers_df['costo'].astype(float)
	
    #Verificamos que no haya valores NaN
    if updated_carriers_df.isna().any().any():
    	raise ValueError('El DataFrame aún contiene valores NaN o NaT')  
     
    
    print('Verificamos tipos de datos despues de la modificación: \n', updated_carriers_df.dtypes)
    print('\n\n\n')





    print("Registros de entities_rfc actualizados:\n\n")
    print(updated_clientesFacturacion_df.shape)
    print(updated_clientesFacturacion_df[['ID', 'FK_user', 'FK_entities']].head(10))
    print('Columnas del Dataframe antes de la inserción \n:', clientesFacturacion_df.columns)
    print('Verificamos tipos de datos:\n\n', clientesFacturacion_df.dtypes)
    
    #Convertimos a un tipo de dato valido
    updated_clientesFacturacion_df['ID'] = updated_clientesFacturacion_df['ID'].astype(int)  # ID es un entero
    updated_clientesFacturacion_df['FK_user'] = updated_clientesFacturacion_df['FK_user'].astype(int)  # FK_user es un entero
    updated_clientesFacturacion_df['rfc'] = updated_clientesFacturacion_df['rfc'].astype(str)  # rfc es nvarchar(15)
    updated_clientesFacturacion_df['razonSocial'] = updated_clientesFacturacion_df['razonSocial'].astype(str)  # razonSocial es nvarchar(400)
    updated_clientesFacturacion_df['contacto'] = updated_clientesFacturacion_df['contacto'].astype(str)  # contacto es nvarchar(200)
    updated_clientesFacturacion_df['calle'] = updated_clientesFacturacion_df['calle'].astype(str)  # calle es nvarchar(300)
    updated_clientesFacturacion_df['numExt'] = updated_clientesFacturacion_df['numExt'].astype(str)  # numExt es nvarchar(50)
    updated_clientesFacturacion_df['numInt'] = updated_clientesFacturacion_df['numInt'].astype(str)  # numInt es nvarchar(50)
    updated_clientesFacturacion_df['colonia'] = updated_clientesFacturacion_df['colonia'].astype(str)  # colonia es nvarchar(300)
    updated_clientesFacturacion_df['municipio'] = updated_clientesFacturacion_df['municipio'].astype(str)  # municipio es nvarchar(300)
    updated_clientesFacturacion_df['localidad'] = updated_clientesFacturacion_df['localidad'].astype(str)  # localidad es nvarchar(300)
    updated_clientesFacturacion_df['entidadFederativa'] = updated_clientesFacturacion_df['entidadFederativa'].astype(str)  # entidadFederativa es nchar(200)
    updated_clientesFacturacion_df['codigoPostal'] = updated_clientesFacturacion_df['codigoPostal'].astype(str)  # codigoPostal es nvarchar(50)
    updated_clientesFacturacion_df['telefonoFijo'] = updated_clientesFacturacion_df['telefonoFijo'].astype(str)  # telefonoFijo es nvarchar(30)
    updated_clientesFacturacion_df['telefonoCelular'] = updated_clientesFacturacion_df['telefonoCelular'].astype(str)  # telefonoCelular es nvarchar(50)
    updated_clientesFacturacion_df['correoElectronico'] = updated_clientesFacturacion_df['correoElectronico'].astype(str)  # correoElectronico es nvarchar(200)
    #updated_clientesFacturacion_df['cedula'] = updated_clientesFacturacion_df['cedula'].astype(str)  # cedula es de tipo image (manejar como bytes si es necesario)
    updated_clientesFacturacion_df['validado'] = updated_clientesFacturacion_df['validado'].astype(int)  # validado es un entero
    updated_clientesFacturacion_df['info'] = updated_clientesFacturacion_df['info'].astype(str)  # info es nvarchar(250)
    updated_clientesFacturacion_df['activo'] = updated_clientesFacturacion_df['activo'].astype(bool)  # activo es un bit (booleano)
    updated_clientesFacturacion_df['activoCliente'] = updated_clientesFacturacion_df['activoCliente'].astype(bool)  # activoCliente es un bit (booleano)
    updated_clientesFacturacion_df['extencion'] = updated_clientesFacturacion_df['extencion'].astype(str)  # extencion es nvarchar(10)
    updated_clientesFacturacion_df['FK_entities'] = updated_clientesFacturacion_df['FK_entities'].astype(int)  # FK_entities es un entero
    
    
    
    #Verificamos que no haya valores NaN
    if clientesFacturacion_df.isna().any().any():
    	raise ValueError('El DataFrame aún contiene valores NaN o NaT')   
    
    print('Verificamos tipos de datos despues de la modificación: \n', updated_clientesFacturacion_df)
    print('\n\n\n')


    
    #Verificamos que no haya valores NaN
    if updated_clientesFacturacion_df.isna().any().any():
    	raise ValueError('El DataFrame aún contiene valores NaN o NaT')
    	
    print('Verificamos tipos de datos despues de la modificación (clientes facturacion): \n', updated_clientesFacturacion_df.dtypes)
    print('\n\n\n')
    
    
    
    
    
    # Insertar los datos en la base de datos destino
    insert_dataframe(entities_df, "entities", dst_hook)
    insert_users_dataframe(updated_users_df, "users", dst_hook)
    insert_carriers_dataframe(updated_carriers_df, "entities_carriers", dst_hook)
    insert_clientesFacturacion_dataframe(updated_clientesFacturacion_df, "clientesFacturacion", dst_hook)
   
    
    return entities_df, updated_users_df, updated_carriers_df, updated_clientesFacturacion_df

# Definir el DAG
dag = DAG(
    'heartbeat_newfusion_prod',
    description='Procesa entidades y actualiza claves foráneas',
    #schedule_interval = "10 11 * * *",
    #schedule_interval = "10 6 * * *",
    schedule_interval=None,
    start_date=datetime(2025, 2, 21),
    catchup=False,
)

# Crear el operador de Python
process_task = PythonOperator(
    task_id='process_entities',
    python_callable=process_entities,
    provide_context=True,
    dag=dag,
)


#Definimos flujo de tareas

# Tarea de inicio
start_alert_task = start_alert()

# Tarea de éxito
success_alert_task = success_alert(result=process_task.output)

# Tarea de falla
failure_alert_task = failure_alert()

# Definir las dependencias
start_alert_task >> process_task
process_task >> [success_alert_task, failure_alert_task]




















