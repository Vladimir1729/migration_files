import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='heartbeat_validation_pagatae',
    default_args=default_args,
    description='Validation',
    schedule_interval=None,
    tags=['Validation'],
) as dag:

    def validation_tree():
        print("[INFO] Iniciando proceso de validación de árboles...")
        
        try:
            # Leemos mapeo
            print("[INFO] Leyendo archivo de mapeo...")
            mapeo = pd.read_csv('/root/Documents/airdemo/id_mapping_entities_pagatae_actual.csv')
            print(f"[SUCCESS] Archivo de mapeo leído correctamente. Total de registros: {len(mapeo)}")
            
            # Conexiones
            print("[INFO] Estableciendo conexiones a bases de datos...")
            #origin_hook = MsSqlHook(mssql_conn_id='admin_migration_pagatae_origin')
            origin_hook = MsSqlHook(mssql_conn_id='admin_origin_final_pagatae')
            #dest_hook = MsSqlHook(mssql_conn_id='admin_migration_pagatae_destiny')
            dest_hook = MsSqlHook(mssql_conn_id='admin_destiny_final_pagatae')
            print("[SUCCESS] Conexiones establecidas correctamente")

            # Query base
            query_base = """
            WITH RecursiveTree AS (
                SELECT e.*, e.id AS root_id
                FROM entities e
                WHERE e.id = {id} AND e.deleted = 0

                UNION ALL

                SELECT e.*, rt.root_id
                FROM entities e
                INNER JOIN RecursiveTree rt ON e.parentid = rt.id
                WHERE e.deleted = 0
            )
            SELECT rt.root_id, COUNT(e.ID) AS total_clientes_en_red
            FROM RecursiveTree rt
            JOIN entities e ON rt.root_id = e.id
            GROUP BY rt.root_id
            """

            errores = []
            total_registros = len(mapeo)
            print(f"[INFO] Iniciando validación de {total_registros} registros...")

            for i, row in mapeo.iterrows():
                original_id = row['original_id']
                new_id = row['new_id']
                
                print(f"[PROGRESO] Procesando registro {i+1}/{total_registros} - ID original: {original_id}, ID nuevo: {new_id}")

                query_origen = query_base.format(id=original_id)
                query_destino = query_base.format(id=new_id)

                print(f"[DEBUG] Ejecutando consulta en origen para ID {original_id}...")
                resultado_origen = origin_hook.get_first(query_origen)
                print(f"[DEBUG] Resultado origen: {resultado_origen}")

                print(f"[DEBUG] Ejecutando consulta en destino para ID {new_id}...")
                resultado_destino = dest_hook.get_first(query_destino)
                print(f"[DEBUG] Resultado destino: {resultado_destino}")

                # Validación para evitar error si alguno es None
                if resultado_origen is None or resultado_destino is None:
                    error_msg = f"[WARNING] Uno de los resultados es None para ID original {original_id} / nuevo {new_id}"
                    print(error_msg)
                    errores.append({
                        'original_id': original_id,
                        'new_id': new_id,
                        'origen': resultado_origen,
                        'destino': resultado_destino,
                        'error': 'Uno de los resultados es None'
                    })
                    continue

                if resultado_origen[1] != resultado_destino[1]:
                    error_msg = f"[ERROR] Discrepancia encontrada para ID original {original_id} / nuevo {new_id}: Origen={resultado_origen[1]}, Destino={resultado_destino[1]}"
                    print(error_msg)
                    errores.append({
                        'original_id': original_id,
                        'new_id': new_id,
                        'origen': resultado_origen[1],
                        'destino': resultado_destino[1],
                    })
                else:
                    print(f"[SUCCESS] Validación exitosa para ID original {original_id} / nuevo {new_id}")

            if errores:
                error_count = len(errores)
                success_count = total_registros - error_count
                print(f"[RESULTADO] Validación completada con {error_count} errores y {success_count} éxitos")
                print(f"[ERRORES DETALLADOS] {errores}")
                raise ValueError(f"Estructuras diferentes detectadas en {error_count} registros: {errores}")
            else:
                print("[SUCCESS] ¡Todas las validaciones fueron exitosas! No se encontraron discrepancias.")

        except Exception as e:
            print(f"[CRITICAL] Error inesperado durante la validación: {str(e)}")
            raise

    validar = PythonOperator(
        task_id='comparar_estructura_arbol',
        python_callable=validation_tree
    )