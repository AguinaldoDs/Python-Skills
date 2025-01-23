#!/usr/bin/env python
# coding: utf-8
# In[1]:
from PySide6.QtWidgets import (QApplication,
                               QMainWindow,
                               QWidget,
                               QPushButton,
                               QLabel,
                               QVBoxLayout,
                               QTabWidget,
                               QTableWidget,
                               QTableWidgetItem,
                               QComboBox,
                               QMessageBox,
                               QFrame,
                               QSplashScreen,
                               QDialog
                               )
from PySide6.QtGui import (QIcon,
                           QFont,
                           QAction,
                           QKeySequence,
                           QClipboard,
                           QPixmap,
                           QPainter)
import sqlalchemy as sa
import sys
from PySide6.QtCore import Qt, QTimer, QProcess
import pandas as pd
import pyodbc
from PIL import Image, ImageQt
# In[2]:
app = QApplication()
window = QMainWindow()
widget = QWidget()
buttonSelect = QPushButton()
label = QLabel()
pop_up_var = QMessageBox()
font = QFont()
linha = QFrame(widget)
# In[3]:
window.setStyleSheet("""
                    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 rgba(6,49,79,1),
                                stop:1 rgba(9,96,121,1))}
                    """)
# In[4]:
#Tabela de retorno
tabelaRetorno = QTableWidget(24,11,parent=widget)
tabelaRetorno.setFixedSize(1150, 586)
tabelaRetorno.move(155,65)
tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
#css
tabelaRetorno.setStyleSheet("""
                            background-color:white;
                            color: black;
                            """)
header = tabelaRetorno.horizontalHeader()
header.setStyleSheet("""QHeaderView::section {background-color: #092635; 
                                            color: white}""")
tail = tabelaRetorno.verticalHeader()
tail.setStyleSheet("""QHeaderView::section {background-color: #092635; 
                                            color: white}""")
# ## Button Mes
# In[5]:
butaoMes = QComboBox(widget)
butaoMes.setFixedSize(60,25)
butaoMes.move(19,104)
butaoMes.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
Mes = [str(i) for i in range(1,13)]
butaoMes.addItems(Mes)
# In[6]:
label_mes = QLabel(widget)
label_mes.setText('Mês')
label_mes.setFixedSize(45,25)
label_mes.move(25,80)
label_mes.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_mes.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# ## Button Ano
# In[7]:
from datetime import datetime
Ano = 2025
butaoAno = QComboBox(widget)
butaoAno.setFixedSize(60,25)
butaoAno.move(79,104)
butaoAno.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
#atribui e cria index
butaoAno.addItem(str(Ano))
butaoAno.setCurrentIndex(0)
# 
# In[8]:
label_ano = QLabel(widget)
label_ano.setText('Ano')
label_ano.setFixedSize(35,25)
label_ano.move(85,80)
label_ano.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_ano.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# ## Button Importar - Func
# In[9]:
#botao importar 
#atribui botao
butao_importar = QPushButton(widget)
#p e t
butao_importar.setFixedSize(75,35)
butao_importar.move(37,371)
icon = (r'\\172.17.1.115\temp\Temp_py\carregar.png')
icon_var = QIcon(icon)
butao_importar.setIcon(icon_var)
butao_importar.setStyleSheet("""
                                background-color: white;
                                border-radius: 5px;
                            """)
# In[10]:
label_importar = QLabel(widget)
label_importar.setText('Importar')
label_importar.setFixedSize(59,25)
label_importar.move(45,346)
label_importar.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_importar.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[11]:
def mouse_sobre_botao(butao_importar):
    butao_importar.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_importar):
    butao_importar.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_importar.enterEvent = lambda event: mouse_sobre_botao(butao_importar)
butao_importar.leaveEvent = lambda event: mouse_fora_botao(butao_importar)
# In[12]:
#funções para linhas importadas ou não
def importada():
    pop_up_var.information(widget,'Informação',f'Importado com sucesso!!!')
def nao_importada():
    pop_up_var.information(widget,'Informação',f'Sem linhas disponiveis para importação')
# In[13]:
#$#$
def send_to_database(tabelaRetorno):
    
    index_mes = butaoMes.currentIndex()
    selected_option_mes = butaoMes.itemText(index_mes)
    
    # Obter dados da QTableWidget
    rows = tabelaRetorno.rowCount()
    cols = tabelaRetorno.columnCount()
    data = []
    for row in range(rows):
        # Verificar se a primeira coluna está vazia
        if tabelaRetorno.item(row, 0) is None or tabelaRetorno.item(row, 0).text() == '':
            continue  # Pular a iteração se a primeira coluna estiver vazia
        row_data = [tabelaRetorno.item(row, col).text() if tabelaRetorno.item(row, col) is not None else '' for col in range(cols)]
        data.append(row_data)
    # Criar um DataFrame pandas
    df = pd.DataFrame(data, columns=[tabelaRetorno.horizontalHeaderItem(col).text() for col in range(cols)])
    
    # Informações de conexão
    connection_string = 'DRIVER={SQL Server};SERVER=172.17.1.115;DATABASE=dbdatastorehouse;UID=USR_MIS;PWD=MIS@321'
    
    # Conectar ao banco de dados SQL Server
    connection = pyodbc.connect(connection_string)
    # Nome da tabela no SQL Server e esquema
    tabela_sql = 'TabelaFrasesGlobalPy'
    esquema_sql = 'dbo'
    agora = datetime.now()
    # Formatar a data e hora com segundos
    formato_com_segundos = "%Y-%m-%d %H:%M:%S"
    data_com_segundos = agora.strftime(formato_com_segundos)
    df['data_importacao'] = data_com_segundos
    df['mes'] = selected_option_mes
    df['Ano'] = 2025
    
    # Criar um cursor para executar comandos SQL
    cursor = connection.cursor()
    for index, row in df.iterrows():
        # Verificar se há valores vazios (None) na linha
        if row.isnull().any():
            continue
        # Construir a parte de valores da instrução SQL
        valores = ', '.join([f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'" if isinstance(value, pd.Timestamp) else f"'{value}'" for value in row])
        # Montar e imprimir o comando SQL
        sql = f"INSERT INTO {esquema_sql}.{tabela_sql} VALUES ({valores})"
        # Executar o comando SQL apenas se não houver valores vazios
        cursor.execute(sql)
    # Commit para salvar as alterações
    connection.commit()
    
    #retorno para o usuario
    tabelaRetorno.clear()
    tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
    
    importada()
butao_importar.clicked.connect(lambda: send_to_database(tabelaRetorno))
# ## Button Clear table
# In[14]:
#botao importar 
butao_clear = QPushButton(widget)
#p e t
butao_clear.setFixedSize(75,35)
butao_clear.move(37,442)
icon_clear = (r'\\172.17.1.115\temp\\Temp_py\limpar-limpo.png')
icon_var_clear = QIcon(icon_clear)
butao_clear.setIcon(icon_var_clear)
butao_clear.setStyleSheet(""" 
                        background: white;
                        color: white;
                        border-radius: 5px;
                        """)
# In[15]:
label_clear = QLabel(widget)
label_clear.setText('Limpar')
label_clear.setFixedSize(59,25)
label_clear.move(45,417)
label_clear.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_clear.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# ## Tabela resume esta sendo criada aqui
# In[16]:
tabela_resumo = QTableWidget(1,4,parent=widget)
tabela_resumo.setStyleSheet("""
                            background-color: white;
                            color: black;
                            """)
#Altera cursor
def mouse_sobre_botao(butao_clear):
    butao_clear.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
#Retorna cursor ao normal
def mouse_fora_botao(butao_clear):
    butao_clear.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
#função para limpeza da tabela 
def clear_tabela_retorno():
    tabelaRetorno.clear()
    tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
    
    tabela_resumo.clear()
    tabela_resumo.setHorizontalHeaderLabels(['Vol. Importações','Ultima Data','Vol. Frases disponiveis',"""Quantidade DU's"""])
butao_clear.clicked.connect(clear_tabela_retorno)
# In[17]:
def mouse_sobre_botao(butao_clear):
    butao_clear.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_clear):
    butao_clear.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_clear.enterEvent = lambda event: mouse_sobre_botao(butao_clear)
butao_clear.leaveEvent = lambda event: mouse_fora_botao(butao_clear)
# ## Button Pesquisa - Func
# In[18]:
#botao pesquisar
#atribui botao
butao_pesquisar = QPushButton(widget)
#p e t
butao_pesquisar.setFixedSize(75,35)
butao_pesquisar.move(37,529)
#add icon
icon = (r'\\172.17.1.115\temp\\Temp_py\lupa.png')
icon_tratado = QIcon(icon)
butao_pesquisar.setIcon(icon_tratado)
butao_pesquisar.setStyleSheet("""
                                background-color: white;
                                border-radius: 5px;
                              """)
# In[19]:
def mouse_sobre_botao(butao_pesquisar):
    butao_pesquisar.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_pesquisar):
    butao_pesquisar.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_pesquisar.enterEvent = lambda event: mouse_sobre_botao(butao_pesquisar)
butao_pesquisar.leaveEvent = lambda event: mouse_fora_botao(butao_pesquisar)
# In[20]:
label_pesquisar = QLabel(widget)
label_pesquisar.setText('Pesquisar')
label_pesquisar.setFixedSize(59,25)
label_pesquisar.move(45,505)
label_pesquisar.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_pesquisar.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white; 
                                              
                        """)
#border-radius: 5px;
# ## Button Banco
# In[21]:
butao_banco = QComboBox(widget)
#p e t
butao_banco.setFixedSize(120,25)
butao_banco.move(19,164)
txt = ('Bradesco','BV','Alure')
butao_banco.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
#texto
butao_banco.addItems(txt)
# In[22]:
label_banco = QLabel(widget)
label_banco.setText('Banco')
label_banco.setFixedSize(100,25)
label_banco.move(30,140)
label_banco.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_banco.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[23]:
def mouse_sobre_botao(butao_banco):
    butao_banco.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_banco):
    butao_banco.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_banco.enterEvent = lambda event: mouse_sobre_botao(butao_banco)
butao_banco.leaveEvent = lambda event: mouse_fora_botao(butao_banco)
# ## Button Carteira - Func vinc a button banco
# In[24]:
#botao Carteira
#atribui botao
butao_carteira = QComboBox(widget)
#p e t
butao_carteira.setFixedSize(120,25)
butao_carteira.move(19,224)
butao_carteira.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
# In[25]:
label_carteira = QLabel(widget)
label_carteira.setText('Carteira')
label_carteira.setFixedSize(100,25)
label_carteira.move(30,200)
label_carteira.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_carteira.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[26]:
bradesco_carteiras = ('Ativos','Bankpar','Credito Imobiliario','Comercial','Dejur - C.Imobiliario',
                      'Financiamentos','Saude','Losango','Veiculos')
bv_carteiras = ('Administrativo','Bom pagador','Credito Imobiliario','Cartao','Cartao CL',
                'Reneg e C.Pessoal','Saldo Remanescente','Solar','Visao Unica','Wo e Contencioso',)
alure_carteiras = ('Daycoval','Recovery','Valia')
def VerificaCarteira(index):
    # Função chamada quando o primeiro QComboBox é alterado
    selected_option = butao_banco.itemText(index)
    # Verifica o banco e retorna Carteiras 
    butao_carteira.clear()
    if selected_option == "Bradesco":
        butao_carteira.addItems(bradesco_carteiras)
    elif selected_option == "BV":
        butao_carteira.addItems(bv_carteiras)
    elif selected_option == "Alure":
        butao_carteira.addItems(alure_carteiras)
#Conecta a caralha do Botão Banco x Carteira
butao_banco.currentIndexChanged.connect(VerificaCarteira)
# ## Button auto completar - Func
# In[27]:
butao_autocompl = QPushButton(widget)
#p e t
butao_autocompl.setFixedSize(75,35)
butao_autocompl.move(37,300)
icon = (r'\\172.17.1.115\temp\Temp_py\cadastro.png')
icon_tratado = QIcon(icon)
butao_autocompl.setStyleSheet(""" 
                        background: white;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
butao_autocompl.setIcon(icon_tratado)
# In[28]:
label_autocompl = QLabel(widget)
label_autocompl.setText('Preencher')
label_autocompl.setFixedSize(59,25)
label_autocompl.move(45,275)
label_autocompl.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_autocompl.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[29]:
def mouse_sobre_botao(butao_autocompl):
    butao_autocompl.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_autocompl):
    butao_autocompl.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_autocompl.enterEvent = lambda event: mouse_sobre_botao(butao_autocompl)
butao_autocompl.leaveEvent = lambda event: mouse_fora_botao(butao_autocompl)
def botao_carteira_vazio():
    pop_up_var.information(widget,'Informação','Selecione uma carteira!')
# In[30]:
def auto_compl_():
    index_mes = butaoMes.currentIndex()
    selected_option_mes = butaoMes.itemText(index_mes)
    index_mes = butaoAno.currentIndex()
    selected_option_ano = butaoAno.itemText(index_mes)
    #path
    conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
    database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    # cursor
    engine_retorno2 = sa.create_engine(database_url2)
    # conn
    conn = engine_retorno2.connect()
    #query
    query = (f"""
                select 
                    convert(date,data) data,
                    DiaUtil
                from dbdatastorehouse.geral.calendario with(nolock)
                where DiaUtil is not null
                and mes = {selected_option_mes}
                and ano = {selected_option_ano}
             """)
    query_tratada = pd.read_sql(query,conn)
    tabelaRetorno.clear()
    tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
    #for para colocar na tabela [DATA]
    Coluna1_data = query_tratada['data'].tolist()
    for linha, valor in enumerate(Coluna1_data):
        item = QTableWidgetItem(str(valor))
        tabelaRetorno.setItem(linha, 0, item)
    
     #for para colocar na tabela [DU]
    Coluna1_Du = query_tratada['DiaUtil'].tolist()
    
    for linha, valor in enumerate(Coluna1_Du):
        item = QTableWidgetItem(str(valor))
        tabelaRetorno.setItem(linha, 1, item)
    
    #for para colocar na tabel [BANCO]
    
    index_banco = butao_banco.currentIndex()
    selected_option_banco = butao_banco.itemText(index_banco)
    
    for linha, valor in enumerate(Coluna1_Du):
        item = QTableWidgetItem(str(selected_option_banco))
        tabelaRetorno.setItem(linha, 2, item)
    
    #for para colocar na tabel [CARTEIRA]
    index_carteira = butao_carteira.currentIndex()
    selected_option_carteira = butao_carteira.itemText(index_carteira)
    if selected_option_carteira != "":
        for linha, valor in enumerate(Coluna1_Du):
            item = QTableWidgetItem(str(selected_option_carteira))
            tabelaRetorno.setItem(linha, 3, item)
    elif selected_option_carteira =="": 
        tabelaRetorno.clear()
        tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
        botao_carteira_vazio()
butao_autocompl.clicked.connect(auto_compl_)
    
# ## Button Delete - Func
# In[31]:
#botao Delete 
butao_delete = QPushButton(widget)
#p e t
butao_delete.setFixedSize(75,35)
butao_delete.move(37,599)
icon = (r'\\172.17.1.115\temp\\Temp_py\lixeira-de-reciclagem.png')
icon_tratado = QIcon(icon)
butao_delete.setIcon(icon_tratado)
butao_delete.setStyleSheet("""
                            border-radius: 5px;
                            background-color: white; 
                           """)
# In[32]:
label_delete = QLabel(widget)
label_delete.setText('Deletar')
label_delete.setFixedSize(59,25)
label_delete.move(45,575)
label_delete.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_delete.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[33]:
def mouse_sobre_botao(butao_delete):
    butao_delete.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_delete):
    butao_delete.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_delete.enterEvent = lambda event: mouse_sobre_botao(butao_delete)
butao_delete.leaveEvent = lambda event: mouse_fora_botao(butao_delete)
# In[34]:
#retorno tabela vazia
def tabela_vazia():
    pop_up_var.information(widget,'Informação','Tabela vazia!')
def tabela_vazia_delete():
    pop_up_var.information(widget,'Informação','Nenhuma linha disponivel para exclusão!')
#retorno delete com sucesso
def valida_delete(linhas):
    pop_up_var.information(widget,'Delete',f'Total de {linhas} linhas deletadas com sucesso!')
#função para deletar
def deleta_casos():
    index_mes = butaoMes.currentIndex()
    selected_option_mes = butaoMes.itemText(index_mes)
    index_banco = butao_banco.currentIndex()
    selected_option_banco = butao_banco.itemText(index_banco)
    index_carteira = butao_carteira.currentIndex()
    selected_option_carteira = butao_carteira.itemText(index_carteira)
    
    conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
    database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    # cursor
    engine_delete = sa.create_engine(database_url2)
    
    # conn
    conn_delete = engine_delete.connect()
    
    # Select/delete
    data_atual = datetime.now()
    ano = data_atual.year
    consulta_retorno2 = sa.text(f"""
                                delete a 
                                from dbdatastorehouse.dbo.TabelaFrasesGlobalPy a
                                where a.mes = '{selected_option_mes}'
                                and a.ano = '2025'
                                and a.banco = '{selected_option_banco}'
                                and a.carteira = '{selected_option_carteira}'
                            """)
    linhas_paramentro = sa.text(f"""
                                select count(1) 
                                from dbdatastorehouse.dbo.TabelaFrasesGlobalPy a
                                where a.mes = '{selected_option_mes}'
                                and a.ano = '2025'
                                and a.banco = '{selected_option_banco}'
                                and a.carteira = '{selected_option_carteira}'
                            """)
    # Executando a consulta com parâmetros
    resultados_linhas = conn_delete.execute(linhas_paramentro).scalar()
    linhas = resultados_linhas
    # Limpar a tabela antes de adicionar novos itens
    tabelaRetorno.clear()
    tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
    #Se estiver vazio retorna o pop up de vazio
    if linhas == 0:
        tabela_vazia_delete()
    #retorno pop up delete
    else: 
        valida_delete(linhas=linhas)
        conn_delete.execute(consulta_retorno2)
        
    conn_delete.commit()
# confere botao
def exibir_pop_up():
    mensagem = "Tem certeza que deseja excluir?"
    resposta = QMessageBox.question(None, "Pop-up", mensagem, QMessageBox.Yes | QMessageBox.No)
    # Verifica a resposta
    if resposta == QMessageBox.Yes:
        deleta_casos()
    else:
        print('')
butao_delete.clicked.connect(exibir_pop_up)
## Button Pesquisar
# In[35]:
def verifica_mes_b_pesquisar():    
    try:
        conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
        database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
        # cursor
        engine_pesquisar = sa.create_engine(database_url2)
        # conn
        conn_pesquisar = engine_pesquisar.connect()
        tabelaRetorno.clear()
        index_mes = butaoMes.currentIndex()
        selected_option_mes = butaoMes.itemText(index_mes)
        index_banco = butao_banco.currentIndex()
        selected_option_banco = butao_banco.itemText(index_banco)
        index_carteira = butao_carteira.currentIndex()
        selected_option_carteira = butao_carteira.itemText(index_carteira)
        # Select
        data_atual = datetime.now()
        ano = data_atual.year
        consulta_retorno2 = sa.text(f"""
            SELECT
                [Data],
                [Du],
                [Banco],
                [Carteira],
                [Frase],
                [Var1],
                [Var2],
                [Var3],
                [Var4],
                [Var5],
                [Segmentacao]
            FROM dbdatastorehouse.dbo.TabelaFrasesGlobalPy with(nolock)
            WHERE mes = '{selected_option_mes}'
            AND ano = '2025'
            AND Banco = '{selected_option_banco}'
            AND CARTEIRA = '{selected_option_carteira}'
        """)
        # Executando a consulta com parâmetros
        resultados = conn_pesquisar.execute(consulta_retorno2)
        print(resultados)
        frame_inserir_tb = pd.DataFrame(resultados)
        # # Parametros do tamanho da tabela
        parametro_df_row = frame_inserir_tb.shape
        parametro_df_row_tratado_row = int(parametro_df_row[0])
        parametro_df_row_tratado_columns = int(parametro_df_row[1])
        tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
        # # Tratar tabela para receber os retornos do banco
        if parametro_df_row_tratado_row !=0:
            for linhas in range(parametro_df_row_tratado_row):
                for columns in range(parametro_df_row_tratado_columns):
                    item = QTableWidgetItem(str(frame_inserir_tb.iloc[linhas, columns]))
                    tabelaRetorno.setItem(linhas, columns, item)
        elif parametro_df_row_tratado_row ==0:
            tabela_vazia()  
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
butao_pesquisar.clicked.connect(verifica_mes_b_pesquisar)
# In[36]:
#tabela resumo superior
tabela_resumo.setFixedSize(563,55)
tabela_resumo.move(445,5)
tabela_resumo.setFont(font)
#css
header = tabela_resumo.horizontalHeader()
header.setStyleSheet("QHeaderView::section { background-color: #092635; color: white}")
tail = tabela_resumo.verticalHeader()
tail.setStyleSheet("QHeaderView::section { background-color: #092635; color: white}")
#nomes dessa bosta
tabela_resumo.setHorizontalHeaderLabels(['Vol. Importações','Ultima Data','Vol. Frases disponiveis',"""Quantidade DU's"""])
tabela_resumo.setVerticalHeaderLabels([])
tabela_resumo.verticalHeader().setVisible(False)
# ## Func Resumo Superior
# 
# In[37]:
# def resumo_superior():
def resumo_resumo():
    index_mes = butaoMes.currentIndex()
    selected_option_mes = butaoMes.itemText(index_mes)
    index_banco = butao_banco.currentIndex()
    selected_option_banco = butao_banco.itemText(index_banco)
    index_carteira = butao_carteira.currentIndex()
    selected_option_carteira = butao_carteira.itemText(index_carteira)
    conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
    database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    # cursor
    engine_pesquisar_resumo = sa.create_engine(database_url2)
    # conn
    conn_pesquisar_resumo = engine_pesquisar_resumo.connect()
    #select
    
    text = sa.text(f"""
                    select
                        count(distinct Data_Importacao),
                        MAX(convert(datetime,data_importacao)),
                        COUNT(case when frase <> '' then Frase end),
                        COUNT(Du)
                    from [dbdatastorehouse].[dbo].[tabelafrasesglobalpy] with(nolock)
                    where mes = {selected_option_mes}
                    and banco = '{selected_option_banco}'
                    and carteira = '{selected_option_carteira}'
                """)
    
    df = pd.read_sql_query(text, conn_pesquisar_resumo)
# Converter o DataFrame para uma lista
    result_list = df.values.flatten().tolist()
    for col, value in enumerate(result_list):
        item = QTableWidgetItem(str(value))
        tabela_resumo.setItem(0, col, item)
butao_pesquisar.clicked.connect(resumo_resumo)
# In[38]:
# Ação para colar
paste_action = QAction("Colar", tabelaRetorno)
paste_action.setShortcut(QKeySequence.Paste)
# Cria a função
def on_paste_action_triggered():
    # Suponha que você queira colar o texto nas células selecionadas
    selected_ranges = tabelaRetorno.selectedRanges()
    Clipboard = QApplication.clipboard()
    Clipboard_text = Clipboard.text()
    CLipTratada  = [line.split('\t') for line in Clipboard_text.strip('\n').split('\n')]
    line = len(CLipTratada)
    columns = len(CLipTratada[0])
    for selected_range in selected_ranges:
        top_row = selected_range.topRow()
        left_column = selected_range.leftColumn()
        for row in range(top_row, top_row+line):
            for col in range(left_column, left_column+columns):
                # Obtém o item existente ou cria um novo se não existir
                item = tabelaRetorno.item(row, col)
                if item is None:
                    item = QTableWidgetItem()
                    tabelaRetorno.setItem(row, col, item)
                
                item.setText(CLipTratada[row - top_row][col-left_column])
paste_action.triggered.connect(on_paste_action_triggered)
# Adiciona a ação à tabela
tabelaRetorno.addAction(paste_action)
tabelaRetorno.setContextMenuPolicy(Qt.ActionsContextMenu)
# In[39]:
linha.setStyleSheet("""
                    background-color: white;
                    """)
linha.setFixedSize(125,1)
linha.move(15,65)
# In[40]:
linha2 = QFrame(widget)
linha2.setStyleSheet("""
                    background-color: white;
                    """)
linha2.setFixedSize(125,1)
linha2.move(15,260)
# In[41]:
linha3 = QFrame(widget)
linha3.setStyleSheet("""background-color: white;""")
linha3.setFixedSize(125,1)
linha3.move(15,490)
# In[42]:
label_filtros = QLabel(widget)
label_filtros.setText("Filtros")
label_filtros.setFixedSize(90,40)
label_filtros.move(30,10)
label_filtros.setAlignment(Qt.AlignCenter)
label_filtros.setStyleSheet("""
                            color: white;
                            border: 1px solid white;
                            border-radius:5px;
                            """)
# In[43]:
central_widget = QWidget(window)
window.setCentralWidget(central_widget)
central_widget.setFixedSize(1500,1500)
central_widget.move(0,0)
# Layout para o widget central
main_layout = QVBoxLayout(central_widget)
# QTabWidget para abas
tab_widget = QTabWidget()
# Conteúdo da Aba 1
tab1_content = widget
tab_widget.addTab(tab1_content, "Calendario Frases")
main_layout.addWidget(tab_widget)
# #### ABA 2 (INICIA AQUI)
# In[44]:
widget2 = QWidget()
tab2_content = widget2
tab_widget.addTab(tab2_content, "Calendario Volumetrias")
main_layout.addWidget(tab_widget)
#tab2_content.setFixedSize(300, 200)
# In[45]:
label_filtros_wd2 = QLabel(widget2)
label_filtros_wd2.setText("Filtros")
label_filtros_wd2.setFixedSize(90,40)
label_filtros_wd2.move(30,10)
label_filtros_wd2.setAlignment(Qt.AlignCenter)
label_filtros_wd2.setStyleSheet("""
                                color: white;
                                border-radius:5px;
                                border: 1px solid white;
                                """)
# In[46]:
linha_wd2 = QFrame(widget2)
linha_wd2.setStyleSheet("""
                    background-color: white;
                    """)
linha_wd2.setFixedSize(125,1)
linha_wd2.move(15,65)
# In[47]:
#tabela resumo superior
tabela_resumo_wd2 = QTableWidget(1,4,parent=widget2)
tabela_resumo_wd2.setFixedSize(562,55)
tabela_resumo_wd2.move(445,5)
tabela_resumo_wd2.setFont(font)
#css
tabela_resumo_wd2.setStyleSheet("""
                                background-color: white;
                                """)
header_wd2 = tabela_resumo_wd2.horizontalHeader()
header_wd2.setStyleSheet("QHeaderView::section { background-color: #092635; color: white}")
tail_wd2 = tabela_resumo_wd2.verticalHeader()
tail_wd2.setStyleSheet("QHeaderView::section { background-color: #092635; color: white}")
#nomes dessa bosta
tabela_resumo_wd2.setHorizontalHeaderLabels(['Vol. Importações','Ultima Data','Vol. Disponiveis',"""Quantidade DU's"""])
tabela_resumo_wd2.setVerticalHeaderLabels([])
tabela_resumo_wd2.verticalHeader().setVisible(False)
# In[48]:
#Tabela de retorno
tabelaRetorno_wd2 = QTableWidget(24,14,parent=widget2)
tabelaRetorno_wd2.setFixedSize(1150, 586)
tabelaRetorno_wd2.move(155,65)
tabelaRetorno_wd2.setHorizontalHeaderLabels(["Data","Du","Banco","Carteira","FX1","FX2","FX3","FX4","FX5","FX6","FX7","FX8","FX9","FX10"])
#css
tabelaRetorno_wd2.setStyleSheet("""
                                background-color:white;
                                color:brack;
                                """)
header_wd2 = tabelaRetorno_wd2.horizontalHeader()
header_wd2.setStyleSheet("""QHeaderView::section {background-color: #092635; 
                                            color: white}""")
tail_wd2 = tabelaRetorno_wd2.verticalHeader()
tail_wd2.setStyleSheet("""QHeaderView::section {background-color: #092635; 
                                            color: white}""")
# ## Botão Mês/Ano Wd2
# In[49]:
#Mes
butaoMes_wds = QComboBox(widget2)
butaoMes_wds.setFixedSize(60,25)
butaoMes_wds.move(19,104)
butaoMes_wds.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
Mes = [str(i) for i in range(1,13)]
butaoMes_wds.addItems(Mes)
# In[50]:
#Mes
label_mes = QLabel(widget2)
label_mes.setText('Mês')
label_mes.setFixedSize(45,25)
label_mes.move(25,80)
label_mes.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_mes.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[51]:
#Ano
Ano = 2025
butaoAno_wd2 = QComboBox(widget2)
butaoAno_wd2.setFixedSize(60,25)
butaoAno_wd2.move(79,104)
butaoAno_wd2.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
#atribui e cria index
butaoAno_wd2.addItem(str(Ano))
butaoAno_wd2.setCurrentIndex(0)
# In[52]:
#Ano
label_ano = QLabel(widget2)
label_ano.setText('Ano')
label_ano.setFixedSize(35,25)
label_ano.move(85,80)
label_ano.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_ano.setStyleSheet(""" background: #092635;
                            color: white;
                            border: 1px solid white;""")
# ## Banco Wd2
# In[53]:
butao_banco_wd2 = QComboBox(widget2)
#p e t
butao_banco_wd2.setFixedSize(120,25)
butao_banco_wd2.move(19,164)
txt = ('Bradesco','BV','Alure')
butao_banco_wd2.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
#texto
butao_banco_wd2.addItems(txt)
# In[54]:
label_banco_wd2 = QLabel(widget2)
label_banco_wd2.setText('Banco')
label_banco_wd2.setFixedSize(100,25)
label_banco_wd2.move(30,140)
label_banco_wd2.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_banco_wd2.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# ## Carteira Wd2
# In[55]:
#botao Carteira
#atribui botao
butao_carteira_wd2 = QComboBox(widget2)
#p e t
butao_carteira_wd2.setFixedSize(120,25)
butao_carteira_wd2.move(19,224)
butao_carteira_wd2.setStyleSheet(""" 
                                    background: #092635;
                                    color: white;
                                    border: 1px solid white;
                                    border-radius: 5px;
                                    """)
# In[56]:
label_carteira = QLabel(widget2)
label_carteira.setText('Carteira')
label_carteira.setFixedSize(100,25)
label_carteira.move(30,200)
label_carteira.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_carteira.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[57]:
bradesco_carteiras_wd2 = ('Ativos','Bankpar','Credito Imobiliario','Comercial','Dejur - C.Imobiliario',
                      'Financiamentos','Saude','Losango','Veiculos')
bv_carteiras_wd2 = ('Administrativo','Bom pagador','Credito Imobiliario','Cartao','Cartao CL',
                'Reneg e C.Pessoal','Saldo Remanescente','Solar','Visao Unica','Wo e Contencioso',)
alure_carteiras_wd2 = ('Daycoval','Recovery','Valia')
def VerificaCarteira_wd2(index):
    # Função chamada quando o primeiro QComboBox é alterado
    selected_option_wd2 = butao_banco_wd2.itemText(index)
    # Verifica o banco e retorna Carteiras 
    butao_carteira_wd2.clear()
    if selected_option_wd2 == "Bradesco":
        butao_carteira_wd2.addItems(bradesco_carteiras_wd2)
    elif selected_option_wd2 == "BV":
        butao_carteira_wd2.addItems(bv_carteiras_wd2)
    elif selected_option_wd2 == "Alure":
        butao_carteira_wd2.addItems(alure_carteiras_wd2)
#Conecta a caralha do Botão Banco x Carteira
butao_banco_wd2.currentIndexChanged.connect(VerificaCarteira_wd2)
# In[58]:
linha_wd2 = QFrame(widget2)
linha_wd2.setStyleSheet("""background-color: white;""")
linha_wd2.setFixedSize(125,1)
linha_wd2.move(15,260)
# ## Botão auto-complet wd2
# In[59]:
butao_autocompl_wd2 = QPushButton(widget2)
#p e t
butao_autocompl_wd2.setFixedSize(75,35)
butao_autocompl_wd2.move(37,300)
icon = (r'\\172.17.1.115\temp\Temp_py\cadastro.png')
icon_tratado = QIcon(icon)
butao_autocompl_wd2.setStyleSheet(""" 
                        background: white;
                        color: white;
                        border: 1px solid white;
                        border-radius: 5px;
                        """)
butao_autocompl_wd2.setIcon(icon_tratado)
# In[60]:
label_autocompl_wd2 = QLabel(widget2)
label_autocompl_wd2.setText('Preencher')
label_autocompl_wd2.setFixedSize(59,25)
label_autocompl_wd2.move(45,275)
label_autocompl_wd2.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_autocompl_wd2.setStyleSheet(""" 
                                    background: #092635;
                                    color: white;
                                    border: 1px solid white;
                                    """)
# In[61]:
def mouse_sobre_botao(butao_autocompl_wd2):
    butao_autocompl_wd2.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_autocompl_wd2):
    butao_autocompl_wd2.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_autocompl_wd2.enterEvent = lambda event: mouse_sobre_botao(butao_autocompl_wd2)
butao_autocompl_wd2.leaveEvent = lambda event: mouse_fora_botao(butao_autocompl_wd2)
def botao_carteira_vazio():
    pop_up_var.information(widget2,'Informação','Selecione uma carteira!')
# In[62]:
def auto_compl_wd2():
    index_mes = butaoMes_wds.currentIndex()
    selected_option_mes = butaoMes_wds.itemText(index_mes)
    index_mes = butaoAno_wd2.currentIndex()
    selected_option_ano = butaoAno_wd2.itemText(index_mes)
    #path
    conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
    database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    # cursor
    engine_retorno2 = sa.create_engine(database_url2)
    # conn
    conn = engine_retorno2.connect()
    #query
    query = (f"""
                select 
                    convert(date,data) data,
                    DiaUtil
                from dbdatastorehouse.geral.calendario with(nolock)
                where DiaUtil is not null
                and mes = {selected_option_mes}
                and ano = {selected_option_ano}
             """)
    query_tratada = pd.read_sql(query,conn)
    tabelaRetorno_wd2.clear()
    tabelaRetorno_wd2.setHorizontalHeaderLabels(["Data","Du","Banco","Carteira","FX1","FX2","FX3","FX4","FX5","FX6","FX7","FX8","FX9","FX10"])
    
    #for para colocar na tabela [DATA]
    Coluna1_data = query_tratada['data'].tolist()
    for linha, valor in enumerate(Coluna1_data):
        item = QTableWidgetItem(str(valor))
        tabelaRetorno_wd2.setItem(linha, 0, item)
    
     #for para colocar na tabela [DU]
    Coluna1_Du = query_tratada['DiaUtil'].tolist()
    
    for linha, valor in enumerate(Coluna1_Du):
        item = QTableWidgetItem(str(valor))
        tabelaRetorno_wd2.setItem(linha, 1, item)
    
    #for para colocar na tabel [BANCO]
    
    index_banco = butao_banco_wd2.currentIndex()
    selected_option_banco = butao_banco_wd2.itemText(index_banco)
    
    for linha, valor in enumerate(Coluna1_Du):
        item = QTableWidgetItem(str(selected_option_banco))
        tabelaRetorno_wd2.setItem(linha, 2, item)
    
    #for para colocar na tabel [CARTEIRA]
    index_carteira = butao_carteira_wd2.currentIndex()
    selected_option_carteira = butao_carteira_wd2.itemText(index_carteira)
    if selected_option_carteira != "":
        for linha, valor in enumerate(Coluna1_Du):
            item = QTableWidgetItem(str(selected_option_carteira))
            tabelaRetorno_wd2.setItem(linha, 3, item)
    elif selected_option_carteira =="": 
        tabelaRetorno_wd2.clear()
        tabelaRetorno_wd2.setHorizontalHeaderLabels(["Data","Du","Banco","Carteira","FX1","FX2","FX3","FX4","FX5","FX6","FX7","FX8","FX9","FX10"])
        botao_carteira_vazio()
butao_autocompl_wd2.clicked.connect(auto_compl_wd2)
# ## Importar Calendario Volumetria
# In[63]:
#botao importar 
#atribui botao
butao_importar_wd2 = QPushButton(widget2)
#p e t
butao_importar_wd2.setFixedSize(75,35)
butao_importar_wd2.move(37,371)
icon = (r'\\172.17.1.115\temp\Temp_py\carregar.png')
icon_var = QIcon(icon)
butao_importar_wd2.setIcon(icon_var)
butao_importar_wd2.setStyleSheet("""
                                background-color: white;
                                border-radius: 5px;
                            """)
# In[64]:
label_importar_wd2 = QLabel(widget2)
label_importar_wd2.setText('Importar')
label_importar_wd2.setFixedSize(59,25)
label_importar_wd2.move(45,346)
label_importar_wd2.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_importar_wd2.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[65]:
def mouse_sobre_botao(butao_importar_wd2):
    butao_importar_wd2.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_importar_wd2):
    butao_importar_wd2.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_importar_wd2.enterEvent = lambda event: mouse_sobre_botao(butao_importar_wd2)
butao_importar_wd2.leaveEvent = lambda event: mouse_fora_botao(butao_importar_wd2)
# In[66]:
#funções para linhas importadas ou não
def importada():
    pop_up_var.information(widget2,'Informação',f'Importado com sucesso!!!')
def nao_importada():
    pop_up_var.information(widget2,'Informação',f'Sem linhas disponiveis para importação')
# In[67]:
#$#$
def send_to_database_wd2(tabelaRetorno_wd2):
    
    index_mes = butaoMes_wds.currentIndex()
    selected_option_mes = butaoMes_wds.itemText(index_mes)
    
    # Obter dados da QTableWidget
    rows = tabelaRetorno_wd2.rowCount()
    cols = tabelaRetorno_wd2.columnCount()
    data = []
    for row in range(rows):
        # Verificar se a primeira coluna está vazia
        if tabelaRetorno_wd2.item(row, 0) is None or tabelaRetorno_wd2.item(row, 0).text() == '':
            continue  # Pular a iteração se a primeira coluna estiver vazia
        row_data = [tabelaRetorno_wd2.item(row, col).text() if tabelaRetorno_wd2.item(row, col) is not None else '' for col in range(cols)]
        data.append(row_data)
    # Criar um DataFrame pandas
    df = pd.DataFrame(data, columns=[tabelaRetorno_wd2.horizontalHeaderItem(col).text() for col in range(cols)])
    
    # Informações de conexão
    connection_string = 'DRIVER={SQL Server};SERVER=172.17.1.115;DATABASE=dbdatastorehouse;UID=USR_MIS;PWD=MIS@321'
    
    # Conectar ao banco de dados SQL Server
    connection = pyodbc.connect(connection_string)
    # Nome da tabela no SQL Server e esquema
    tabela_sql = 'TabelaCalendarioSmsGlobalPy'
    esquema_sql = 'dbo'
    agora = datetime.now()
    # Formatar a data e hora com segundos
    formato_com_segundos = "%Y-%m-%d %H:%M:%S"
    data_com_segundos = agora.strftime(formato_com_segundos)
    df['data_importacao'] = data_com_segundos
    df['mes'] = selected_option_mes
    df['Ano'] = 2025
    
    # Criar um cursor para executar comandos SQL
    cursor = connection.cursor()
    for index, row in df.iterrows():
        # Verificar se há valores vazios (None) na linha
        if row.isnull().any():
            continue
        # Construir a parte de valores da instrução SQL
        valores = ', '.join([f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'" if isinstance(value, pd.Timestamp) else f"'{value}'" for value in row])
        valores = valores.replace(".","")
        # Montar e imprimir o comando SQL
        sql = f"INSERT INTO {esquema_sql}.{tabela_sql} VALUES ({valores})"
        # Executar o comando SQL apenas se não houver valores vazios
        cursor.execute(sql)
    # Commit para salvar as alterações
    connection.commit()
    
    #retorno para o usuario
    tabelaRetorno_wd2.clear()
    tabelaRetorno_wd2.setHorizontalHeaderLabels(["Data","Du","Banco","Carteira","FX1","FX2","FX3","FX4","FX5","FX6","FX7","FX8","FX9","FX10"])
    
    importada()
butao_importar_wd2.clicked.connect(lambda: send_to_database_wd2(tabelaRetorno_wd2=tabelaRetorno_wd2))
# In[68]:
linha_wd2 = QFrame(widget2)
linha_wd2.setStyleSheet("""background-color: white;""")
linha_wd2.setFixedSize(125,1)
linha_wd2.move(15,490)
# ## Botão Clear
# In[69]:
#botao importar
butao_clear_wd2 = QPushButton(widget2)
#p e t
butao_clear_wd2.setFixedSize(75,35)
butao_clear_wd2.move(37,442)
icon_clear = (r'\\172.17.1.115\temp\Temp_py\limpar-limpo.png')
icon_var_clear = QIcon(icon_clear)
butao_clear_wd2.setIcon(icon_var_clear)
butao_clear_wd2.setStyleSheet(""" 
                                background: white;
                                color: white;
                                border-radius: 5px;
                                """)
# In[70]:
label_clear_wd2 = QLabel(widget2)
label_clear_wd2.setText('Limpar')
 
label_clear_wd2.setFixedSize(59,25)
label_clear_wd2.move(45,417)
label_clear_wd2.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_clear_wd2.setStyleSheet(""" 
                            background: #092635;
                            color: white;
                            border: 1px solid white;
                            """)
# In[71]:
#Altera cursor
def mouse_sobre_botao(butao_clear_wd2):
    butao_clear_wd2.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
#Retorna cursor ao normal
def mouse_fora_botao(butao_clear_wd2):
    butao_clear_wd2.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
#função para limpeza da tabela 
def clear_tabela_retorno_wd2():
    tabelaRetorno_wd2.clear()
    tabelaRetorno_wd2.setHorizontalHeaderLabels(["Data","Du","Banco","Carteira","FX1","FX2","FX3","FX4","FX5","FX6","FX7","FX8","FX9","FX10"])
    
    tabela_resumo_wd2.clear()
    tabela_resumo_wd2.setHorizontalHeaderLabels(['Vol. Importações','Ultima Data','Vol. Frases disponiveis',"""Quantidade DU's"""])
butao_clear_wd2.clicked.connect(clear_tabela_retorno_wd2)
# In[72]:
def mouse_sobre_botao(butao_clear_wd2):
    butao_clear_wd2.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_clear_wd2):
    butao_clear_wd2.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_clear_wd2.enterEvent = lambda event: mouse_sobre_botao(butao_clear_wd2)
butao_clear_wd2.leaveEvent = lambda event: mouse_fora_botao(butao_clear_wd2)
# ## Button Pesquisar
# In[73]:
#botao pesquisar
#atribui botao
butao_pesquisar_wd2 = QPushButton(widget2)
#p e t
butao_pesquisar_wd2.setFixedSize(75,35)
butao_pesquisar_wd2.move(37,529)
#add icon
icon = (r'\\172.17.1.115\temp\Temp_py\lupa.png')
icon_tratado = QIcon(icon)
butao_pesquisar_wd2.setIcon(icon_tratado)
butao_pesquisar_wd2.setStyleSheet("""
                                background-color: white;
                                border-radius: 5px;
                              """)
# In[74]:
label_pesquisar_wd2 = QLabel(widget2)
label_pesquisar_wd2.setText('Pesquisar')
label_pesquisar_wd2.setFixedSize(59,25)
label_pesquisar_wd2.move(45,505)
label_pesquisar_wd2.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_pesquisar_wd2.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white; 
                                              
                        """)
#border-radius: 5px;
# In[75]:
def mouse_sobre_botao(butao_pesquisar_wd2):
    butao_pesquisar_wd2.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_pesquisar_wd2):
    butao_pesquisar_wd2.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_pesquisar_wd2.enterEvent = lambda event: mouse_sobre_botao(butao_pesquisar_wd2)
butao_pesquisar_wd2.leaveEvent = lambda event: mouse_fora_botao(butao_pesquisar_wd2)
# In[76]:
def verifica_mes_b_pesquisar_wd2():    
    try:
        conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
        database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
        # cursor
        engine_pesquisar = sa.create_engine(database_url2)
        # conn
        conn_pesquisar = engine_pesquisar.connect()
        tabelaRetorno_wd2.clear()
        index_mes = butaoMes_wds.currentIndex()
        selected_option_mes = butaoMes_wds.itemText(index_mes)
        index_banco = butao_banco_wd2.currentIndex()
        selected_option_banco = butao_banco_wd2.itemText(index_banco)
        index_carteira = butao_carteira_wd2.currentIndex()
        selected_option_carteira = butao_carteira_wd2.itemText(index_carteira)
        # Select
        data_atual = datetime.now()
        ano = data_atual.year
        consulta_retorno2 = sa.text(f"""
                     SELECT
                        [Data]
                        ,[Du]
                        ,[Banco]
                        ,[Carteira]
                        ,[FX1]
                        ,[FX2]
                        ,[FX3]
                        ,[FX4]
                        ,[FX5]
                        ,[FX6]
                        ,[FX7]
                        ,[FX8]
                        ,[FX9]
                        ,[FX10]
                        ,[Data_Importacao]
                    FROM [dbdatastorehouse].[dbo].[TabelaCalendarioSmsGlobalPy] with(nolock)
                    WHERE mes = '{selected_option_mes}'
                        AND ano = '2025'
                        AND Banco = '{selected_option_banco}'
                        AND CARTEIRA = '{selected_option_carteira}'
                """)
        # Executando a consulta com parâmetros
        resultados = conn_pesquisar.execute(consulta_retorno2)
        print(resultados)
        frame_inserir_tb = pd.DataFrame(resultados)
        # # Parametros do tamanho da tabela
        parametro_df_row = frame_inserir_tb.shape
        parametro_df_row_tratado_row = int(parametro_df_row[0])
        parametro_df_row_tratado_columns = int(parametro_df_row[1])
        tabelaRetorno_wd2.setHorizontalHeaderLabels(["Data","Du","Banco","Carteira","FX1","FX2","FX3","FX4","FX5","FX6","FX7","FX8","FX9","FX10"])
        # # Tratar tabela para receber os retornos do banco
        if parametro_df_row_tratado_row !=0:
            for linhas in range(parametro_df_row_tratado_row):
                for columns in range(parametro_df_row_tratado_columns):
                    item = QTableWidgetItem(str(frame_inserir_tb.iloc[linhas, columns]))
                    tabelaRetorno_wd2.setItem(linhas, columns, item)
        elif parametro_df_row_tratado_row ==0:
            tabela_vazia()  
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
butao_pesquisar_wd2.clicked.connect(verifica_mes_b_pesquisar_wd2)
# ## Button delete
# In[77]:
#botao Delete 
butao_delete_wd2 = QPushButton(widget2)
#p e t
butao_delete_wd2.setFixedSize(75,35)
butao_delete_wd2.move(37,599)
icon = (r'\\172.17.1.115\temp\Temp_py\lixeira-de-reciclagem.png')
icon_tratado = QIcon(icon)
butao_delete_wd2.setIcon(icon_tratado)
butao_delete_wd2.setStyleSheet("""
                            border-radius: 5px;
                            background-color: white; 
                           """)
# In[78]:
label_delete_wd2 = QLabel(widget2)
label_delete_wd2.setText('Deletar')
label_delete_wd2.setFixedSize(59,25)
label_delete_wd2.move(45,575)
label_delete_wd2.setAlignment(Qt.AlignmentFlag.AlignCenter)
label_delete_wd2.setStyleSheet(""" 
                        background: #092635;
                        color: white;
                        border: 1px solid white;
                        """)
# In[79]:
def mouse_sobre_botao(butao_delete_wd2):
    butao_delete_wd2.setCursor(Qt.PointingHandCursor)  # Altera o cursor para uma mão apontando
def mouse_fora_botao(butao_delete_wd2):
    butao_delete_wd2.setCursor(Qt.ArrowCursor)  # Restaura o cursor padrão (seta)
butao_delete_wd2.enterEvent = lambda event: mouse_sobre_botao(butao_delete_wd2)
butao_delete_wd2.leaveEvent = lambda event: mouse_fora_botao(butao_delete_wd2)
# In[80]:
#retorno tabela vazia
def tabela_vazia():
    pop_up_var.information(widget2,'Informação','Tabela vazia!')
def tabela_vazia_delete():
    pop_up_var.information(widget2,'Informação','Nenhuma linha disponivel para exclusão!')
#retorno delete com sucesso
def valida_delete(linhas):
    pop_up_var.information(widget,'Delete',f'Total de {linhas} linhas deletadas com sucesso!')
#função para deletar
def deleta_casos_wd2():
    index_mes = butaoMes.currentIndex()
    selected_option_mes = butaoMes.itemText(index_mes)
    index_banco = butao_banco.currentIndex()
    selected_option_banco = butao_banco.itemText(index_banco)
    index_carteira = butao_carteira.currentIndex()
    selected_option_carteira = butao_carteira.itemText(index_carteira)
    
    conn_str = r"Driver=SQL Server;" f"Server=172.17.1.115;" r"trusted_connection=yes;"
    database_url2 = f"mssql+pyodbc:///?odbc_connect={conn_str}"
    # cursor
    engine_delete = sa.create_engine(database_url2)
    
    # conn
    conn_delete = engine_delete.connect()
    
    # Select/delete
    data_atual = datetime.now()
    ano = data_atual.year
    consulta_retorno2 = sa.text(f"""
                                delete a 
                                from dbdatastorehouse.dbo.TabelaCalendarioSmsGlobalPy a
                                where a.mes = '{selected_option_mes}'
                                and a.ano = '2025'
                                and a.banco = '{selected_option_banco}'
                                and a.carteira = '{selected_option_carteira}'
                            """)
    linhas_paramentro = sa.text(f"""
                                select count(1) 
                                from dbdatastorehouse.dbo.TabelaCalendarioSmsGlobalPy a with(nolock)
                                where a.mes = '{selected_option_mes}'
                                and a.ano = '2025'
                                and a.banco = '{selected_option_banco}'
                                and a.carteira = '{selected_option_carteira}'
                            """)
    # Executando a consulta com parâmetros
    resultados_linhas = conn_delete.execute(linhas_paramentro).scalar()
    linhas = resultados_linhas
    # Limpar a tabela antes de adicionar novos itens
    tabelaRetorno.clear()
    tabelaRetorno.setHorizontalHeaderLabels(["Data", "Du", "Banco", "Carteira", "Frase", "Var1", "Var2", "Var3", "Var4", "Var5","Segmentacao"])
    #Se estiver vazio retorna o pop up de vazio
    if linhas == 0:
        tabela_vazia_delete()
    #retorno pop up delete
    else: 
        valida_delete(linhas=linhas)
        conn_delete.execute(consulta_retorno2)
        
    conn_delete.commit()
butao_delete_wd2.clicked.connect(deleta_casos_wd2)
    
# ## Func para Colar
# In[81]:
# Ação para colar
paste_action = QAction("Colar", tabelaRetorno_wd2)
paste_action.setShortcut(QKeySequence.Paste)
# Cria a função
def on_paste_action_triggered():
    # Suponha que você queira colar o texto nas células selecionadas
    selected_ranges = tabelaRetorno_wd2.selectedRanges()
    Clipboard = QApplication.clipboard()
    Clipboard_text = Clipboard.text()
    CLipTratada  = [line.split('\t') for line in Clipboard_text.strip('\n').split('\n')]
    line = len(CLipTratada)
    columns = len(CLipTratada[0])
    for selected_range in selected_ranges:
        top_row = selected_range.topRow()
        left_column = selected_range.leftColumn()
        for row in range(top_row, top_row+line):
            for col in range(left_column, left_column+columns):
                # Obtém o item existente ou cria um novo se não existir
                item = tabelaRetorno_wd2.item(row, col)
                if item is None:
                    item = QTableWidgetItem()
                    tabelaRetorno_wd2.setItem(row, col, item)
                
                item.setText(CLipTratada[row - top_row][col-left_column])
paste_action.triggered.connect(on_paste_action_triggered)
# Adiciona a ação à tabela
tabelaRetorno_wd2.addAction(paste_action)
tabelaRetorno_wd2.setContextMenuPolicy(Qt.ActionsContextMenu)
#parametros dessa bosta da tabela de resumo
for col in range(tabela_resumo.columnCount()):
    tabela_resumo.setColumnWidth(col, 140)
#parametros dessa bosta
for col in range(tabela_resumo_wd2.columnCount()):
    tabela_resumo_wd2.setColumnWidth(col, 140)
# In[82]:
#Apresentação tela 
image_path = (r'\\172.17.1.115\temp\Temp_py\logo_mis.jpg') #caminho
duration = 5000  #duração
image = Image.open(image_path)
pixmap = ImageQt.toqpixmap(image)
splash = QSplashScreen(pixmap, Qt.WindowStaysOnTopHint | Qt.FramelessWindowHint)
splash.show()
def callback():
    window.showMaximized()
   
#sequencia de timer
timer = QTimer(splash)
timer.timeout.connect(splash.close)
timer.timeout.connect(callback)
timer.start(duration)
app.exec()