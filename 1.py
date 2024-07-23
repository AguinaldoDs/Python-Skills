import datetime
import time
import pandas as pd
import pyodbc
import schedule

def execute_job():
    data = datetime.datetime.now().date().strftime("%Y%m%d")
    
    # f. Path
    path = rf"Z:\\Rollout\\R_{data}.csv"
    
    # option
    commit = 'replace'  # Option: { replace, insert }
    
    # f. read
    try:
        read_f = pd.read_csv(path)
        valid_f = True
    except FileNotFoundError:
        valid_f = False
    
    if valid_f == True:
        # connect db
        lkd_server = '172.17.1.35'
        db = 'dw'
        usr = 'USR_MIS'
        pwd = 'MIS@321'
    
        path_db = f'DRIVER={{SQL Server}};SERVER={lkd_server};DATABASE={db};UID={usr};PWD={pwd}'
    
        connection = pyodbc.connect(path_db)
        cursor = connection.cursor()
    
        # valida se já foi importado
        valid_dup = cursor.execute("""
            SELECT COUNT(1) AS Return
            FROM [recovery].[rollout_20240717]
            WHERE CONVERT(DATE, DataImportacao) = CONVERT(DATE, GETDATE())
        """)
        return_valid = valid_dup.fetchone()[0]
        
        if return_valid == 0:
            # Ativar fast_executemany
            cursor.fast_executemany = True
    
            # Reordenar as colunas do DataFrame para corresponder à ordem esperada na tabela
            desired_order = ['DsAgencia', 'IdCliente', 'IdCaso', 'dsranking', 'DescontoAntigo', 'DescontoNovo', 'DESCONTO']
            read_f = read_f[desired_order]
    
            # Defina o tamanho do lote
            batch_size = 10000
    
            if commit == 'replace':
                cursor.execute("TRUNCATE TABLE [recovery].[rollout_20240717]")
                connection.commit()  # Confirma a execução do truncate
    
                for start in range(0, len(read_f), batch_size):
                    end = start + batch_size
                    batch = read_f.iloc[start:end]
                    
                    # Preparar os dados para inserção
                    data_to_insert = [
                        (row['DsAgencia'], row['IdCliente'], row['IdCaso'], row['dsranking'], 
                         row['DescontoAntigo'], row['DescontoNovo'], row['DESCONTO'])
                        for index, row in batch.iterrows()
                    ]
    
                    cursor.executemany(
                        """
                        INSERT INTO [recovery].[rollout_20240717] 
                        (DsAgencia, IdCliente, IdCaso, dsranking, DescontoAntigo, DescontoNovo, DESCONTO, DataImportacao) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
                        """,
                        data_to_insert
                    )
                    
                    connection.commit()  # Confirma as mudanças após cada lote
                print('rodou replace')
    
            if commit == 'insert':
                for start in range(0, len(read_f), batch_size):
                    end = start + batch_size
                    batch = read_f.iloc[start:end]
                    
                    # Preparar os dados para inserção
                    data_to_insert = [
                        (row['DsAgencia'], row['IdCliente'], row['IdCaso'], row['dsranking'],
                         row['DescontoAntigo'], row['DescontoNovo'], row['DESCONTO'])
                        for index, row in batch.iterrows()
                    ]
    
                    cursor.executemany(
                        """
                        INSERT INTO [recovery].[rollout_20240717]
                        (DsAgencia, IdCliente, IdCaso, dsranking, DescontoAntigo, DescontoNovo, DESCONTO) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        data_to_insert
                    )
                    
                    connection.commit()  # Confirma as mudanças após cada lote
                    print('rodou insert')
    
                cursor.close()
                connection.close()
        else:
            print('arquivo não encontrado')    
    else:
        print('Já têm informações importadas para essa data')    
# Agendar a execução do job
schedule.every().day.at("09:00").do(execute_job)
schedule.every().day.at("15:00").do(execute_job)

# Loop infinito para manter o script rodando
while True:
    schedule.run_pending()
    time.sleep(1)
