import json
import sqlite3
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import requests

db_path = "dados_governo.db"
json_path = "dados_brutos.json"
grafico_1 = "grafico_evolucao_mensal.png"
grafico_2 = "grafico_inflacao_anual.png"

ano_inicio = datetime.now().year - 10

def coletar_e_tratar():
    print("[1/5] Coletando dados da API do Banco Central do Brasil...")
    url_ipca = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
    url_selic = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?formato=json"
    
    resp_ipca = requests.get(url_ipca).json()
    resp_selic = requests.get(url_selic).json()
    
    print("[2/5] Salvando dados brutos em JSON e tratando no Pandas...")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"ipca": resp_ipca, "selic": resp_selic}, f, indent=2)
        
    df_ipca = pd.DataFrame(resp_ipca).rename(columns={"valor": "ipca"})
    df_selic = pd.DataFrame(resp_selic).rename(columns={"valor": "selic"})
    
    df = pd.merge(df_ipca, df_selic, on="data")
    
    df["data_dt"] = pd.to_datetime(df["data"], format="%d/%m/%Y")
    df["ipca"] = pd.to_numeric(df["ipca"])
    df["selic"] = pd.to_numeric(df["selic"])
    df["ano"] = df["data_dt"].dt.year
    
    df = df[df["ano"] >= ano_inicio].copy().reset_index(drop=True)
    return df

def calcular_estatisticas(df):
    print("[3/5] Calculando estatísticas gerais...")
    stats = {
        "indicador": ["IPCA", "Selic"],
        "media": [df["ipca"].mean(), df["selic"].mean()],
        "maximo": [df["ipca"].max(), df["selic"].max()],
        "minimo": [df["ipca"].min(), df["selic"].min()]
    }
    
    df_stats = pd.DataFrame(stats)
    print("\nEstatísticas (Últimos 10 anos):")
    print(df_stats.to_string(index=False, float_format="%.2f"))
    print()
    return df_stats

def gerar_graficos(df):
    print("[4/5] Gerando visualizações gráficas...")
    plt.figure(figsize=(10, 5))
    plt.plot(df["data_dt"], df["ipca"], label="IPCA Mensal (%)", color="#e74c3c")
    plt.plot(df["data_dt"], df["selic"], label="Selic Mensal (%)", color="#3498db")
    plt.title("Evolução Mensal: Inflação (IPCA) vs Taxa Selic")
    plt.xlabel("Período")
    plt.ylabel("Taxa (%)")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(grafico_1)
    plt.close()
    
    media_anual = df.groupby("ano")["ipca"].mean().reset_index()
    
    plt.figure(figsize=(10, 5))
    bars = plt.bar(media_anual["ano"], media_anual["ipca"], color="#f39c12")
    plt.title("Média Anual da Inflação (IPCA)")
    plt.xlabel("Ano")
    plt.ylabel("IPCA Médio (%)")
    plt.xticks(media_anual["ano"])
    
    for bar in bars:
        altura = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, altura + 0.05, f"{altura:.2f}%", ha="center", fontsize=9)
        
    plt.tight_layout()
    plt.savefig(grafico_2)
    plt.close()

def salvar_banco(df, df_stats):
    print("[5/5] Armazenando dados no banco SQLite...")
    conn = sqlite3.connect(db_path)
    
    df_limpo = df[["data", "ano", "ipca", "selic"]].copy()
    df_limpo.to_sql("economia_historico", conn, if_exists="replace", index=False)
    
    df_stats.to_sql("economia_estatisticas", conn, if_exists="replace", index=False)
    
    conn.close()

if __name__ == "__main__":
    print(f"=== INICIANDO PIPELINE (Período: {ano_inicio} até {datetime.now().year}) ===")
    df_principal = coletar_e_tratar()
    estatisticas = calcular_estatisticas(df_principal)
    gerar_graficos(df_principal)
    salvar_banco(df_principal, estatisticas)