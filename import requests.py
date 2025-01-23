import requests
import pandas as pd
from datetime import datetime, timedelta

# Substitua pela sua chave de API da Calendarific
API_KEY = 'yrcn8UQI7Qe0XU0BJRShjZTdQh4tUpIW'
COUNTRY = 'BR'  # Brasil
YEAR = 2025

# URL da API Calendarific para buscar os feriados de um ano e país específico
url = f"https://calendarific.com/api/v2/holidays?api_key={API_KEY}&country={COUNTRY}&year={YEAR}"

# Função para verificar se a data é um dia útil (segunda a sexta-feira)
def is_weekday(date):
    return date.weekday() < 5  # Retorna True se for um dia útil

# Função para buscar os feriados da API Calendarific
def fetch_holidays():
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()["response"]["holidays"]
    else:
        raise Exception("Erro ao consultar a API Calendarific")

# Função para gerar todas as datas do ano de 2025
def generate_dates():
    start_date = datetime(YEAR, 1, 1)
    end_date = datetime(YEAR, 12, 31)
    
    dates = []
    current_date = start_date

    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    
    return dates

# Função para gerar a tabela no formato desejado
def generate_table(holidays, dates):
    holiday_dates = {}

    # Itera pelos feriados, tratando diferentes formatos de data
    for holiday in holidays:
        # Verifica se o campo 'date' está no formato esperado
        if isinstance(holiday["date"], dict):
            holiday_date = holiday["date"].get("iso")  # Obtém a data no formato ISO
        else:
            holiday_date = holiday["date"]
        
        holiday_name = holiday.get("name", "Feriado desconhecido")
        
        # Preenche o dicionário com a data do feriado como chave
        if holiday_date:
            holiday_dates[holiday_date] = holiday_name
    
    table_data = []

    for date in dates:
        holiday = holiday_dates.get(date.strftime("%Y-%m-%d"), None)
        
        # Verificar se é um dia útil
        if is_weekday(date):
            day_util = date.weekday() + 1  # Segunda-feira = 1, Terça-feira = 2, ...
        else:
            day_util = None  # Se for sábado ou domingo, DiaUtil será None

        table_data.append({
            "Data": date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "Mes": date.month,
            "Ano": date.year,
            "DiaUtil": day_util,  # Mostra o número do dia útil (1 a 5) ou None para sábado/domingo
            "Feriado": 'S' if holiday else 'N'
        })
    
    # Criar o DataFrame com pandas
    df = pd.DataFrame(table_data)
    return df

# Buscar os feriados para o ano de 2025
holidays = fetch_holidays()

# Gerar todas as datas de 2025
dates_2025 = generate_dates()

# Gerar a tabela com as informações
df = generate_table(holidays, dates_2025)

# Exibir a tabela
print(df)

# Se quiser salvar a tabela em um arquivo CSV
# df.to_csv("calendario_2025.csv", index=False)

df.to_csv("C:/Users/t29969/calendario_2025.csv", index=False)

