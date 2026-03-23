import requests
import pandas as pd
import json
import os
from datetime import datetime

UNSPLASH_ACCESS_KEY = "cJHaVwIeP4qz8VKVRVA3Lx8IV-fwFKGjRHOuBAGoX24"

LATITUDE  = -25.4284
LONGITUDE = -49.2733

hoje        = datetime.today()
data_fim    = hoje.strftime('%Y-%m-%d')
data_inicio = (pd.Timestamp(hoje) - pd.DateOffset(days=30)).strftime('%Y-%m-%d')

print(f'Coletando dados de {data_inicio} até {data_fim}')

# ============================================================
# 2. REQUISIÇÃO À API DE CLIMA (Open-Meteo)
# ============================================================

url_clima = 'https://archive-api.open-meteo.com/v1/archive'

params = {
    'latitude':   LATITUDE,
    'longitude':  LONGITUDE,
    'start_date': data_inicio,
    'end_date':   data_fim,
    'daily':      'temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max',
    'timezone':   'America/Sao_Paulo'
}

resposta = requests.get(url_clima, params=params)

print(f'Status code clima: {resposta.status_code}')
if resposta.status_code == 200:
    print('✅ Dados climáticos obtidos com sucesso!')
else:
    print('❌ Erro na requisição de clima')
    exit()

# ============================================================
# 3. CONVERTENDO PARA DATAFRAME
# ============================================================

dados_brutos = resposta.json()
df = pd.DataFrame(dados_brutos['daily'])
df['time'] = pd.to_datetime(df['time'])

df.rename(columns={
    'time':               'data',
    'temperature_2m_max': 'temp_max',
    'temperature_2m_min': 'temp_min',
    'precipitation_sum':  'precipitacao_mm',
    'windspeed_10m_max':  'vento_max_kmh'
}, inplace=True)

df['amplitude_termica'] = df['temp_max'] - df['temp_min']

def classificar_dia(temp):
    if temp < 15:
        return 'frio'
    elif temp < 25:
        return 'ameno'
    else:
        return 'quente'

df['classificacao'] = df['temp_max'].apply(classificar_dia)

print(f'\nShape do DataFrame: {df.shape}')
print(df.head(5).to_string(index=False))

# ============================================================
# 4. SALVANDO OS DADOS
# ============================================================

df.to_csv('clima_curitiba.csv', index=False)
print('\n✅ CSV salvo: clima_curitiba.csv')

df.to_json('clima_curitiba.json', orient='records', date_format='iso', indent=2)
print('✅ JSON salvo: clima_curitiba.json')

# ============================================================
# 5. BUSCANDO IMAGENS NO UNSPLASH POR CONDIÇÃO CLIMÁTICA
# ============================================================

def buscar_imagem_unsplash(query, access_key, index=0):
    """
    Busca uma imagem no Unsplash com base em uma palavra-chave.
    Retorna um dicionário com url, fotógrafo e link original.
    """
    url = 'https://api.unsplash.com/search/photos'
    headers = {'Authorization': f'Client-ID {access_key}'}
    params  = {'query': query, 'per_page': 5, 'orientation': 'landscape'}

    resposta = requests.get(url, headers=headers, params=params)

    if resposta.status_code != 200:
        print(f'  ❌ Erro Unsplash ({resposta.status_code}): {resposta.text}')
        return None

    resultados = resposta.json().get('results', [])
    if not resultados:
        print(f'  ⚠️  Nenhuma imagem encontrada para "{query}"')
        return None

    foto = resultados[index % len(resultados)]
    return {
        'query':       query,
        'url_imagem':  foto['urls']['regular'],
        'url_thumb':   foto['urls']['thumb'],
        'fotografo':   foto['user']['name'],
        'link_foto':   foto['links']['html'],
        'descricao':   foto.get('alt_description', ''),
    }


def baixar_imagem(url_imagem, nome_arquivo):
    """Faz download da imagem e salva localmente."""
    resposta = requests.get(url_imagem, stream=True)
    if resposta.status_code == 200:
        with open(nome_arquivo, 'wb') as f:
            for chunk in resposta.iter_content(1024):
                f.write(chunk)
        print(f'  💾 Imagem salva: {nome_arquivo}')
    else:
        print(f'  ❌ Falha ao baixar imagem')


# Criar pasta para salvar as imagens
os.makedirs('imagens_clima', exist_ok=True)

# Definir queries por classificação de clima
queries_por_clima = {
    'frio':   'cold winter weather',
    'ameno':  'mild cloudy weather',
    'quente': 'sunny hot weather',
}

print('\n=== Buscando imagens no Unsplash ===')

imagens_coletadas = []

for classificacao, query in queries_por_clima.items():
    print(f'\n🔍 Buscando imagem para: {classificacao} ("{query}")')
    info = buscar_imagem_unsplash(query, UNSPLASH_ACCESS_KEY)

    if info:
        print(f'  📷 Fotógrafo: {info["fotografo"]}')
        print(f'  🔗 Link:      {info["link_foto"]}')

        # Baixar e salvar a imagem
        nome_arquivo = f'imagens_clima/{classificacao}.jpg'
        baixar_imagem(info['url_imagem'], nome_arquivo)

        info['classificacao'] = classificacao
        info['arquivo_local'] = nome_arquivo
        imagens_coletadas.append(info)

# ============================================================
# 6. SALVANDO METADADOS DAS IMAGENS
# ============================================================

df_imagens = pd.DataFrame(imagens_coletadas)
df_imagens.to_csv('imagens_clima/metadados_imagens.csv', index=False)
df_imagens.to_json('imagens_clima/metadados_imagens.json', orient='records', indent=2)

print('\n✅ Metadados das imagens salvos!')
print(df_imagens[['classificacao', 'fotografo', 'link_foto']].to_string(index=False))

# ============================================================
# 7. ANÁLISE: quantos dias de cada classificação tivemos
#    e qual imagem representa cada tipo
# ============================================================

print('\n=== Distribuição dos dias por classificação ===')
contagem = df['classificacao'].value_counts()
print(contagem.to_string())

print('\n=== Resumo final: clima + imagem representativa ===')
for _, row in df_imagens.iterrows():
    total_dias = contagem.get(row['classificacao'], 0)
    print(f"\n🌡️  {row['classificacao'].upper()} — {total_dias} dias nos últimos 30 dias")
    print(f"   Imagem: {row['arquivo_local']}")
    print(f"   Crédito: {row['fotografo']} ({row['link_foto']})")

# ============================================================
# 8. ANÁLISE EXPLORATÓRIA COMPLETA
# ============================================================

print('\n=== Estatísticas Descritivas ===')
print(df[['temp_max', 'temp_min', 'precipitacao_mm', 'vento_max_kmh']].describe().round(2).to_string())

print('\n=== Dias com maior amplitude térmica ===')
print(df.nlargest(5, 'amplitude_termica')[['data', 'temp_min', 'temp_max', 'amplitude_termica']].to_string(index=False))

dias_com_chuva = df[df['precipitacao_mm'] > 0]
print(f'\nDias com chuva: {len(dias_com_chuva)} ({len(dias_com_chuva)/len(df)*100:.1f}%)')
print(f'Total acumulado: {df["precipitacao_mm"].sum():.1f} mm')
print(f'Dia mais chuvoso: {df.loc[df["precipitacao_mm"].idxmax(), "data"].date()} com {df["precipitacao_mm"].max():.1f} mm')

print('\n=== Correlação entre variáveis ===')
print(df[['temp_max', 'temp_min', 'precipitacao_mm', 'vento_max_kmh', 'amplitude_termica']].corr().round(3).to_string())