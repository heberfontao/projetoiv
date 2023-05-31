import pandas as pd
import numpy as np
import os

empresas = ["ABEV3", "AZUL4", "B3SA3", "BBSE3", "BBDC4", "BRAP4", "BBAS3", "BRKM5", "BRFS3", "BPAC11", "CRFB3", "CCRO3", "CMIG4", "CIEL3", "COGN3", "CPLE6", "CSAN3", "CPFE3", "CVCB3", "CYRE3", "ECOR3", "ELET6", "EMBR3", "ENBR3", "ENGI11", "ENEV3", "EGIE3", "EQTL3", "EZTC3", "FLRY3", "GGBR4", "GOAU4", "GOLL4", "NTCO3", "HAPV3", "HYPE3", "ITSA4", "ITUB4", "JBSS3", "JHSF3", "KLBN11", "RENT3", "AMER3", "LREN3", "MGLU3", "MRFG3", "BEEF3", "MRVE3", "MULT3", "PCAR3", "PETR4", "VBBR3", "PRIO3", "QUAL3", "RADL3", "RAIL3", "SBSP3", "SANB11", "CSNA3", "SUZB3", "TAEE11", "VIVT3", "TIMS3", "TOTS3", "UGPA3", "USIM5", "VALE3", "WEGE3", "YDUQ3"]

fundamentos = {}
arquivos = os.listdir('balancos_atual')
for arquivo in arquivos:
    nome = arquivo[-9:-4]
    if '11' in nome:
        nome = arquivo[-10:-4]
    if nome in empresas:
        print(nome)
        balanco = pd.read_excel(f'balancos_atual/{arquivo}', sheet_name=0)
        # na 1º coluna colocar o título com o nome da empresa
        balanco.iloc[0, 0] = nome
        # pegar a 1º linha e tornar cabeçalho
        balanco.columns = balanco.iloc[0]
        balanco = balanco[1:]
        # tornar a 1º coluna (que agora tem o nome da empresa)
        balanco = balanco.set_index(nome)
        dre = pd.read_excel(f'balancos_atual/{arquivo}', sheet_name=1)
        # na 1º coluna colocar o título com o nome da empresa
        dre.iloc[0, 0] = nome
        # pegar a 1º linha e tornar cabeçalho
        dre.columns = dre.iloc[0]
        dre = dre[1:]
        # tornar a 1º coluna (que agora tem o nome da empresa)
        dre = dre.set_index(nome)
        fundamentos[nome] = pd.concat([balanco, dre], axis=0)
#print(fundamentos)

# IMPORTAÇÃO DAS COTAÇÕES
data_inicial = "2012-12-20"
data_final = "2023-03-31"

from pandas_datareader import data as web
import yfinance as yf

yf.pdr_override()
dfs = []

for empresa in empresas:
    acao = empresa
    # acao = 'ABEV3'
    df_ibov = web.get_data_yahoo(acao + '.SA', start=data_inicial, end=data_final)
    df_ibov['Empresa'] = acao
    #display(df_ibov)
    #print(acao)
    dfs.append(df_ibov)
    df_ibov_total = pd.concat(dfs)

df_ibov_total.to_excel(acao + '.xlsx', index=True)

# Acrescetar as datas faltantes no arquivo (dias não úteis), repetindo os valores do dia anterior
import numpy as np

cotacoes_df = pd.read_excel('Cotacoes_atual.xlsx')


# Criar um novo índice com todas as datas desejadas
date_range = pd.date_range(start=cotacoes_df['Date'].min(), end=cotacoes_df['Date'].max(), freq='D')
date_range = date_range.append(pd.Index(['2023-03-31 00:00:00']))  # Adicionar a data não útil
new_index = pd.MultiIndex.from_product([cotacoes_df['Empresa'].unique(), date_range], names=['Empresa', 'Date'])

# Reindexar o DataFrame
cotacoes_df = cotacoes_df.set_index(['Empresa', 'Date']).reindex(new_index)

# Preencher os valores ausentes com o valor anterior válido para cada empresa
cotacoes_df = cotacoes_df.groupby('Empresa').ffill()

# Redefinir o índice para separar "Empresa" e "Date"
cotacoes_df.reset_index(inplace=True)

# Exibir o DataFrame resultante
#print(cotacoes_df)

# Gravar em xlsx
cotacoes_df.to_excel('cotacoes_df.xlsx')

# Ler o arquivo o arquivo de cotações em excel, colocar a Data como index e transformar em datetime
cotacoes_df = pd.read_excel('cotacoes_df.xlsx')
cotacoes = {}
for empresa in cotacoes_df['Empresa'].unique():
    cotacoes[empresa] = cotacoes_df.loc[cotacoes_df['Empresa']==empresa, :]
    cotacoes[empresa] = cotacoes[empresa].set_index('Date')
    cotacoes[empresa].index = pd.to_datetime(cotacoes[empresa].index)

# Excluir empresas com valores nulos
for empresa in empresas:
    if cotacoes[empresa].isnull().values.any():
        cotacoes.pop(empresa)
        fundamentos.pop(empresa)
empresas = list(cotacoes.keys())

# no cotacoes: jogar aas datas para o indice
# no fundamentos:
# trocar linhas por colunas
# tratar as datas para formato de data do python
# juntar os fundamentos com a coluna Adj Close das cotacoes
for empresa in fundamentos:
    tabela = fundamentos[empresa].T
    # display(tabela)
    tabela.index = pd.to_datetime(tabela.index, format="%d/%m/%Y")
    # display(tabela)

    tabela_cotacao = cotacoes[empresa]
    tabela_cotacao = tabela_cotacao[["Adj Close"]]

    tabela = tabela.merge(tabela_cotacao, right_index=True, left_index=True)
    tabela.index.name = empresa
    fundamentos[empresa] = tabela

colunas = list(fundamentos["ABEV3"].columns)

# excluir valores vazios
for empresa in empresas:
        if set(colunas) != set(fundamentos[empresa].columns):
            fundamentos.pop(empresa)

texto_colunas = ";".join(colunas)

colunas_modificadas = []
for coluna in colunas:
    if colunas.count(coluna) == 2 and coluna not in colunas_modificadas:
        texto_colunas = texto_colunas.replace(";" + coluna + ";", ";" + coluna + "_1;", 1)
        colunas_modificadas.append(coluna)
colunas = texto_colunas.split(";")

for empresa in fundamentos:
    fundamentos[empresa].columns = colunas

valores_vazios = dict.fromkeys(colunas, 0)
total_linhas = 0
for empresa in fundamentos:
    tabela = fundamentos[empresa]
    total_linhas += tabela.shape[0]
    for coluna in colunas:
        qtde_vazios = pd.isnull(tabela[coluna]).sum()
        valores_vazios[coluna] += qtde_vazios

remover_colunas =[]
for coluna in valores_vazios:
    if valores_vazios[coluna] > 50:
        remover_colunas.append(coluna)
for empresa in fundamentos:
    fundamentos[empresa] = fundamentos[empresa].drop(remover_colunas, axis=1)
    fundamentos[empresa] = fundamentos[empresa].ffill()

data_inicial = "2012-12-20"
data_final = "2023-03-31"

from pandas_datareader import data as web
import yfinance as yf

yf.pdr_override()

df_ibov = web.get_data_yahoo('^BVSP', start=data_inicial, end=data_final)

impor

datas = fundamentos["ABEV3"].index
for data in datas:
    if data not in df_ibov.index:
        df_ibov.loc[data] = np.nan
df_ibov = df_ibov.sort_index()
df_ibov = df_ibov.ffill()
df_ibov = df_ibov.rename(columns={"Adj Close": "IBOV"})
for empresa in fundamentos:
    fundamentos[empresa] = fundamentos[empresa].merge(df_ibov[["IBOV"]], left_index=True, right_index=True)

# GERAÇÃO DE ARQUIVO CSV COM TODOS OS DADOS ATUAIS


# Criar um DataFrame vazio para armazenar todos os dados dos fundamentos
df_final = pd.DataFrame()

for empresa in fundamentos:
    arq_csv = fundamentos[empresa]
    arq_csv = arq_csv.sort_index()

    # Adicionar coluna com o nome da empresa
    arq_csv['Empresa'] = empresa

    # Concatenar os dados da empresa ao DataFrame final
    df_final = pd.concat([df_final, arq_csv])

# Ordenar o DataFrame pelo índice (datas)
df_final = df_final.sort_index()

# Salvar o DataFrame em um arquivo CSV
df_final.to_csv('fundamentos_empresas.csv', index=True, sep=';', encoding='utf-8', float_format='%.2f')

# tornar os nossos indicadores em percentuais
# fundamentos%tri = fundamento tri / fundamento tri anterior
# cotacao%tri = cotacao tri seguinte / cotacao tri
for empresa in fundamentos:
    fundamento = fundamentos[empresa]
    fundamento = fundamento.sort_index()
    for coluna in fundamento:
        if "Adj Close" in coluna or "IBOV" in coluna:
            pass
        else:
            # pegar cotação anterior
            condicoes = [
                (fundamento[coluna].shift(1) > 0) & (fundamento[coluna] < 0),
                (fundamento[coluna].shift(1) < 0) & (fundamento[coluna] > 0),
                (fundamento[coluna].shift(1) < 0) & (fundamento[coluna] < 0),
                (fundamento[coluna].shift(1) == 0) & (fundamento[coluna] > 0),
                (fundamento[coluna].shift(1) == 0) & (fundamento[coluna] < 0),
                (fundamento[coluna].shift(1) < 0) & (fundamento[coluna] == 0),
            ]
            valores = [
                (fundamento[coluna] / fundamento[coluna].shift(1) + 1),
                (abs(fundamento[coluna] / fundamento[coluna].shift(1) + 1)),
                (abs(fundamento[coluna].shift(1)) - abs(fundamento[coluna])) / abs(fundamento[coluna].shift(1)),
                1,
                -1,
                1,
            ]
            fundamento[coluna] = np.select(condicoes, valores,
                                           default=fundamento[coluna] / fundamento[coluna].shift(1) - 1)

    # pegar cotação seguinte
    fundamento["Adj Close"] = fundamento["Adj Close"].shift(-1) / fundamento["Adj Close"] - 1
    fundamento["IBOV"] = fundamento["IBOV"].shift(-1) / fundamento["IBOV"] - 1
    fundamento["Resultado"] = fundamento["Adj Close"] - fundamento["IBOV"]
    condicoes = [
        (fundamento["Resultado"] > 0),
        (fundamento["Resultado"] < 0) & (fundamento["Resultado"] >= -0.02),
        (fundamento["Resultado"] < -0.02),
    ]
    valores = [2, 1, 0]
    fundamento["Decisão"] = np.select(condicoes, valores)

    fundamentos[empresa] = fundamento


# remover valores vazios
colunas = list(fundamentos["ABEV3"].columns)
valores_vazios = dict.fromkeys(colunas, 0)
total_linhas = 0
for empresa in fundamentos:
    tabela = fundamentos[empresa]
    total_linhas += tabela.shape[0]
    for coluna in colunas:
        qtde_vazios = pd.isnull(tabela[coluna]).sum()
        valores_vazios[coluna] += qtde_vazios
print(valores_vazios)
print(total_linhas)

remover_colunas =[]
for coluna in valores_vazios:
    if valores_vazios[coluna] > (total_linhas / 3):
        remover_colunas.append(coluna)
for empresa in fundamentos:
    fundamentos[empresa] = fundamentos[empresa].drop(remover_colunas, axis=1)
    fundamentos[empresa] = fundamentos[empresa].fillna(0)

copia_fund = fundamentos.copy()
dat = copia_fund