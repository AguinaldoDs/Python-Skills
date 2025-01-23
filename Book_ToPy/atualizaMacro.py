import pyodbc
from datetime import datetime,timedelta
import win32com.client as win32
 
# # String de conexão
# connection_string = (
#     f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#     f"SERVER=172.17.1.115;"
#     f"DATABASE=dbdatastoreHouse;"
#     f"UID=USR_MIS;"
#     f"PWD=MIS@321")


def AtualizaMacro():
    
    # Caminho do arquivo Excel
    caminho_excel = rf"\\172.17.1.115\TEMP\\BookToPy\11.Book Diário CP - Novembro 1.xlsb"

    excel = None
    try:
        # Inicia uma instância do Excel
        excel = win32.Dispatch("Excel.Application")
        excel.Visible = True

        # Abre o arquivo Excel
        workbook = excel.Workbooks.Open(Filename=caminho_excel)
        
        # Habilita as macros
        excel.AutomationSecurity = 1  # Habilita todas as macros
        sheet = workbook.Sheets('Daily')

        sheet.Range("I1").Value = "8" #MES REF 1
        sheet.Range("J1").Value = "9" #MES REF 2
        sheet.Range("K1").Value = "10" #MES REF 3
        

        # Executa a macro
        excel.Application.Run('Atu_All.Atu_All')
                

        # Salva e fecha o arquivo
        workbook.Save()
        workbook.Close(SaveChanges=1)
        

    except Exception as e:
        excel.Application.Quit()
        del excel
    finally:
        excel.Application.Quit()
        del excel


AtualizaMacro()