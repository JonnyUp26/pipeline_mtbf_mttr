import pandas as pd
import re
import numpy as np

# Caminho do seu arquivo

caminho = r'C:\Users\user\OneDrive\Área de Trabalho\João Pedro\Python\MTBF_MTTR\report_2026.xlsx'
caminho2 = r'C:\Users\user\OneDrive\Área de Trabalho\João Pedro\Python\MTBF_MTTR\report_2025.xlsx'


caminhos = [caminho, caminho2]

todos_os_dados = []

for arquivo in caminhos:

    print(f"Lendo arquivo: {arquivo}")
    # Carrega todas as abas do arquivo atual
    todas_abas = pd.read_excel(arquivo, sheet_name=None, skiprows=1)

    for aba in todas_abas:

        df = todas_abas[aba]
        df['data'] = aba
        todos_os_dados.append(df)

    df_completo = pd.concat(todos_os_dados)

# Renomeando as colunas

df_completo.columns.values[0] = 'corredor'
df_completo.columns = [col.replace('\n', '') for col in df_completo.columns]
df_completo.columns = [col.replace(' ', '_') for col in df_completo.columns]
df_completo.columns = [col.lower() for col in df_completo.columns]
df_completo.columns = [col.replace('ç', 'c').replace('ã', 'a').replace('í', 'i').replace('ó', 'o').replace('sensor_desacrificio', '').replace('sensor_deintegridade', '') for col in df_completo.columns]
df_completo.columns = [col.replace('falha_de_comunicacao_entre_o_clp_e_supervisorio(rede_-_telecom)', 'falha_comm') for col in df_completo.columns]
df_completo.columns = [re.sub(r'\(.*?\)', '', col).strip() for col in df_completo.columns]
df_completo.drop(columns=['causa','observacao','acao','colapso','possivel_colapso','tempo_de_avaliacao'], inplace=True)
df_completo.columns = [col.rstrip('_') for col in df_completo.columns]

# Preenchendo pra baixo a coluna corredor

df_completo['corredor'] = df_completo['corredor'].ffill()

# Separação para tratar os dados diarios

df_sensores = df_completo.copy()

df_sensores.drop(columns=['disponibilidade_para_o_cmg','falha_comm','disponibilidade_do_geocod','modo_manutencao','quantidade_de_vezes_que_desabilitou','desabilitado'], inplace=True)

# Criando as colunas de Downtime, uptime e n_falhas

equipamentos = ['gateway', 's1', 's2', 's3', 's4', 's5', 's6', 's7', 's8', 'i1', 'i2', 'i3', 'i4']

id_vars = [c for c in df_sensores.columns if c not in equipamentos]

df_sensores = df_sensores.melt(
    id_vars=id_vars,
    value_vars=equipamentos,
    var_name='equipamento',
    value_name='%_falha'
)

df_sensores['downtime'] = df_sensores['%_falha'] * 1440
df_sensores['uptime'] = 1440 - df_sensores['downtime']
df_sensores['n_falhas'] = np.where(df_sensores['%_falha'] > 0.5,1, 0)
#df_sensores['n_falhas'] = np.where(df_sensores['%_falha'] == 1, df_sensores['%_falha'], 0)
# Dropando colunas originais
df_sensores.drop(columns='%_falha', inplace=True)

# Ajustando a coluna de data
df_sensores['data'] = pd.to_datetime(df_sensores['data'], format='%d.%m.%y', errors='coerce')
df_sensores['mes_ano'] = df_sensores['data'].dt.strftime('%m_%Y')
df_sensores['data'] = df_sensores['data'].dt.strftime('%d/%m/%Y')

# Fazer compilado mensal

df_sensores_mensal = df_sensores.copy()

df_sensores_mensal = df_sensores.groupby(
    ['corredor', 'barragem', 'equipamento']
).agg({
    'data': 'count',
    'downtime': 'sum',
    'uptime': 'sum',
    'n_falhas': 'sum'
}).reset_index()

# MTBF: Quanto tempo ele aguenta rodando antes de dar pau
df_sensores_mensal['mtbf'] = np.where(
    df_sensores_mensal['n_falhas'] > 0,
    df_sensores_mensal['uptime'] / df_sensores_mensal['n_falhas'],
    df_sensores_mensal['uptime'] # Se não falhou, o MTBF é o mês inteiro
)

# MTTR: Qual a média de tempo que ele fica "fora" por falha
df_sensores_mensal['mttr'] = np.where(
    df_sensores_mensal['n_falhas'] > 0,
    df_sensores_mensal['downtime'] / df_sensores_mensal['n_falhas'],
    0 # Se não falhou, o tempo de reparo é zero
)

# Exportando o DataFrame consolidado mensal
df_sensores_mensal.to_csv('relatorio_sensores_mensal.csv',
                          sep=',',
                          index=False,
                          encoding='utf-8-sig')

print("Arquivo exportado com sucesso!")

# Calculando o consumo médio, estoque de segurança e ponto de reposição por corredor

df_sensores_reposicao_corredor = df_sensores_mensal.copy()

df_sensores_reposicao_corredor['tipo'] = df_sensores_reposicao_corredor.apply(lambda x: 'gateway' if x['equipamento'] == 'gateway' else 'sensor', axis=1)

# Se n_falhas for 0, o consumo é 0.
# Caso contrário, aplica a lógica: MTBF <= 0 vira 1/30, e MTBF > 0 calcula 1440/x com teto de 1. - na verdade limitei a 1/30 pq quando varia mt 1 troca deve resovler
df_sensores_reposicao_corredor['consumo_diario'] = df_sensores_reposicao_corredor.apply(
    lambda x: 0 if x['n_falhas'] == 0 else (
        1/x['data'] if x['mtbf'] <= 0 else (1/x['data'] if 1440/x['mtbf'] > 1/x['data'] else 1440/x['mtbf'])
    ), axis=1
)

df_sensores_reposicao_corredor.drop(columns=['downtime','uptime','n_falhas','mtbf','mttr'], inplace=True)

df_sensores_reposicao_corredor['consumo_mensal'] = df_sensores_reposicao_corredor['consumo_diario'] * df_sensores_reposicao_corredor['data']


# Primeiro vou agrupar cada sensor pra ficar uma analise apenas "mensal"
# df de reposicao por barragem caso queira consultar

df_sensores_reposicao_corredor = df_sensores_reposicao_corredor.groupby(
    ['corredor', 'barragem', 'equipamento']
).agg({
    'data': 'sum',
    'consumo_mensal': 'sum'
}).reset_index()

df_sensores_reposicao_corredor['consumo_diario'] = df_sensores_reposicao_corredor['consumo_mensal']/df_sensores_reposicao_corredor['data']

df_sensores_reposicao_corredor = df_sensores_reposicao_corredor.groupby(
    ['corredor']
).agg({
    'consumo_diario': 'sum'
}).reset_index()

LEAD_TIME = 90

df_sensores_reposicao_corredor['estoque_indicado'] = LEAD_TIME * df_sensores_reposicao_corredor['consumo_diario']

print(df_sensores_reposicao_corredor)

print(df_sensores_reposicao_corredor.columns)


# df para posicao por corredor



#print(df_sensores_reposicao_corredor)
#print(df_sensores_reposicao_corredor.columns)



# Tratar dados apenas da barragem
dropar = ['disponibilidade_para_o_cmg', 'falha_comm',
       'modo_manutencao']

df_barragem = df_completo.copy()

df_barragem.drop(columns=equipamentos, inplace=True)
df_barragem.drop(columns=dropar, inplace=True)

df_barragem['uptime'] = (1 - df_barragem['desabilitado'])*1440
df_barragem['downtime'] = (df_barragem['desabilitado'])*1440
df_barragem.drop(columns='disponibilidade_do_geocod', inplace=True)
df_barragem.drop(columns='desabilitado', inplace=True)

# Ajustando a coluna de data
df_barragem['data'] = pd.to_datetime(df_barragem['data'], format='%d.%m.%y', errors='coerce')
df_barragem['mes_ano'] = df_barragem['data'].dt.strftime('%m_%Y')
df_barragem['data'] = df_barragem['data'].dt.strftime('%d/%m/%Y')

df_barragem_mensal = df_barragem.groupby(
    ['corredor', 'barragem','mes_ano']
).agg({
    'data': 'count',
    'downtime': 'sum',
    'uptime': 'sum',
    'quantidade_de_vezes_que_desabilitou': 'sum'
}).reset_index()

# MTBF: Quanto tempo ele aguenta rodando antes de dar pau
df_barragem_mensal['mtbf'] = np.where(
    df_barragem_mensal['quantidade_de_vezes_que_desabilitou'] > 0,
    df_barragem_mensal['uptime'] / df_barragem_mensal['quantidade_de_vezes_que_desabilitou'],
    df_barragem_mensal['uptime'] # Se não falhou, o MTBF é o mês inteiro
)

# MTTR: Qual a média de tempo que ele fica "fora" por falha
df_barragem_mensal['mttr'] = np.where(
    df_barragem_mensal['quantidade_de_vezes_que_desabilitou'] > 0,
    df_barragem_mensal['downtime'] / df_barragem_mensal['quantidade_de_vezes_que_desabilitou'],
    0 # Se não falhou, o tempo de reparo é zero
)

# Exportando o DataFrame consolidado mensal
df_barragem_mensal.to_csv('relatorio_barragem_mensal.csv',
                          sep=',',
                          index=False,
                          encoding='utf-8-sig')

print("Arquivo exportado com sucesso!")

#print(df_compilado.info())
#print(df_compilado.describe())