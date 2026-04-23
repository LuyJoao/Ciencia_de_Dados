import requests
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import json
import os

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "J@ke4682",
    "database": "economia_bcb",
    "port": 3306,
}

BCB_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
SERIES = {"ipca": 433, "selic": 4390, "cambio": 1}
DATA_DIR = "dados"
LOG_FILE = "pipeline.log"

os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def fetch_series(nome, codigo):
    url = BCB_URL.format(codigo=codigo)
    log.info(f"Buscando serie '{nome}' da API do BCB...")
    try:
        resp = requests.get(url, timeout=30, headers={"Accept": "application/json"})
        resp.raise_for_status()
        dados = resp.json()
        log.info(f"{len(dados)} registros obtidos para '{nome}'")
        path = os.path.join(DATA_DIR, f"bruto_{nome}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({nome: dados}, f, ensure_ascii=False, indent=2)
        return dados
    except requests.RequestException as e:
        log.error(f"Erro ao buscar '{nome}': {e}")
        return []


def processar_serie(dados, nome):
    if not dados:
        return pd.DataFrame()

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df.dropna(subset=["data", "valor"], inplace=True)
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df.dropna(subset=["valor"], inplace=True)
    df.rename(columns={"valor": nome}, inplace=True)
    df.sort_values("data", inplace=True)
    df.reset_index(drop=True, inplace=True)

    log.info(f"Serie '{nome}' processada: {len(df)} registros ({df['data'].min().date()} ate {df['data'].max().date()})")
    return df


def conectar_mysql():
    log.info("Conectando ao MySQL...")
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            port=DB_CONFIG["port"],
        )
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        cur.execute(f"USE {DB_CONFIG['database']}")
        conn.database = DB_CONFIG["database"]
        log.info("Conexao estabelecida.")
        return conn
    except mysql.connector.Error as e:
        log.error(f"Erro na conexao: {e}")
        raise


def criar_schema(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS economia_historico (
            id      INT AUTO_INCREMENT PRIMARY KEY,
            data    DATE NOT NULL,
            ano     SMALLINT NOT NULL,
            mes     TINYINT NOT NULL,
            ipca    DECIMAL(8,4),
            selic   DECIMAL(8,4),
            UNIQUE KEY uk_data (data)
        ) ENGINE=InnoDB
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cambio_historico (
            id      INT AUTO_INCREMENT PRIMARY KEY,
            data    DATE NOT NULL UNIQUE,
            usd_brl DECIMAL(10,4) NOT NULL
        ) ENGINE=InnoDB
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS economia_estatisticas (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            indicador     VARCHAR(50) NOT NULL,
            periodo       VARCHAR(20) NOT NULL,
            media         DECIMAL(10,4),
            maximo        DECIMAL(10,4),
            minimo        DECIMAL(10,4),
            desvio_padrao DECIMAL(10,4),
            atualizado_em DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_ind_per (indicador, periodo)
        ) ENGINE=InnoDB
    """)

    conn.commit()
    log.info("Tabelas criadas/verificadas.")


def inserir_historico(conn, df_ipca, df_selic):
    if df_ipca.empty and df_selic.empty:
        log.warning("Sem dados para inserir.")
        return

    frames = []
    if not df_ipca.empty and "ipca" in df_ipca.columns:
        frames.append(df_ipca[["data", "ipca"]].set_index("data"))
    if not df_selic.empty and "selic" in df_selic.columns:
        frames.append(df_selic[["data", "selic"]].set_index("data"))

    if not frames:
        return

    df = pd.concat(frames, axis=1).reset_index().sort_values("data").reset_index(drop=True)
    for col in ["ipca", "selic"]:
        if col not in df.columns:
            df[col] = None

    cur = conn.cursor()
    sql = """
        INSERT INTO economia_historico (data, ano, mes, ipca, selic)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            ipca  = VALUES(ipca),
            selic = VALUES(selic)
    """
    registros = []
    for _, row in df.iterrows():
        registros.append((
            row["data"].date(),
            int(row["data"].year),
            int(row["data"].month),
            None if pd.isna(row.get("ipca")) else float(row["ipca"]),
            None if pd.isna(row.get("selic")) else float(row["selic"]),
        ))

    try:
        cur.executemany(sql, registros)
        conn.commit()
        log.info(f"economia_historico: {cur.rowcount} linhas inseridas/atualizadas.")
    except mysql.connector.Error as e:
        conn.rollback()
        log.error(f"Erro ao inserir historico: {e}")
        raise


def inserir_cambio(conn, df_cambio):
    if df_cambio.empty:
        return

    cur = conn.cursor()
    sql = """
        INSERT INTO cambio_historico (data, usd_brl)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE usd_brl = VALUES(usd_brl)
    """
    registros = [(row["data"].date(), float(row["cambio"])) for _, row in df_cambio.iterrows()]

    try:
        cur.executemany(sql, registros)
        conn.commit()
        log.info(f"cambio_historico: {cur.rowcount} linhas inseridas/atualizadas.")
    except mysql.connector.Error as e:
        conn.rollback()
        log.error(f"Erro ao inserir cambio: {e}")
        raise


def calcular_e_inserir_estatisticas(conn):
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT 'ipca' AS indicador, 'historico_completo' AS periodo,
               AVG(ipca) AS media, MAX(ipca) AS maximo,
               MIN(ipca) AS minimo, STDDEV(ipca) AS desvio_padrao
        FROM economia_historico
        WHERE ipca IS NOT NULL
    """)
    rows = cur.fetchall()

    cur.execute("""
        SELECT 'selic' AS indicador, 'historico_completo' AS periodo,
               AVG(selic) AS media, MAX(selic) AS maximo,
               MIN(selic) AS minimo, STDDEV(selic) AS desvio_padrao
        FROM economia_historico
        WHERE selic IS NOT NULL
    """)
    rows += cur.fetchall()

    cur.execute("""
        SELECT 'ipca' AS indicador, CONCAT('ano_', ano) AS periodo,
               AVG(ipca) AS media, MAX(ipca) AS maximo,
               MIN(ipca) AS minimo, STDDEV(ipca) AS desvio_padrao
        FROM economia_historico
        WHERE ipca IS NOT NULL AND ano >= 2000
        GROUP BY ano
        ORDER BY ano
    """)
    rows += cur.fetchall()

    sql_upsert = """
        INSERT INTO economia_estatisticas (indicador, periodo, media, maximo, minimo, desvio_padrao)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            media         = VALUES(media),
            maximo        = VALUES(maximo),
            minimo        = VALUES(minimo),
            desvio_padrao = VALUES(desvio_padrao)
    """
    dados = [
        (r["indicador"], r["periodo"],
         float(r["media"]) if r["media"] else None,
         float(r["maximo"]) if r["maximo"] else None,
         float(r["minimo"]) if r["minimo"] else None,
         float(r["desvio_padrao"]) if r["desvio_padrao"] else None)
        for r in rows
    ]

    try:
        cur.executemany(sql_upsert, dados)
        conn.commit()
        log.info(f"economia_estatisticas: {cur.rowcount} linhas inseridas/atualizadas.")
    except mysql.connector.Error as e:
        conn.rollback()
        log.error(f"Erro ao inserir estatisticas: {e}")
        raise


def consultar_dados(conn):
    query = "SELECT data, ano, mes, ipca, selic FROM economia_historico ORDER BY data"
    df = pd.read_sql(query, conn, parse_dates=["data"])
    log.info(f"{len(df)} registros carregados do MySQL.")
    return df


def calcular_estatisticas_pandas(df):
    log.info("Calculando estatisticas com pandas...")
    for col in ["ipca", "selic"]:
        s = df[col].dropna()
        if s.empty:
            continue
        log.info(f"{col.upper()}: media={s.mean():.2f}  max={s.max():.2f}  min={s.min():.2f}  dp={s.std():.2f}  mediana={s.median():.2f}")

    anual = df.groupby("ano")[["ipca", "selic"]].mean().round(2)
    log.info(f"Media anual (ultimos 5 anos):\n{anual.tail(5).to_string()}")
    return anual


def gerar_graficos(df, anual):
    plt.style.use("seaborn-v0_8-whitegrid")

    df2000 = df[df["ano"] >= 2000].dropna(subset=["ipca"])
    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(df2000["data"], df2000["ipca"], color="#d62728", linewidth=1.2, label="IPCA (%)")
    ax.axhline(df2000["ipca"].mean(), color="gray", linestyle="--", linewidth=0.9,
               label=f"Media: {df2000['ipca'].mean():.2f}%")
    ax.set_title("IPCA Mensal - Brasil (2000 ate hoje)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Data")
    ax.set_ylabel("Variacao (%)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator(2))
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "grafico1_ipca_linha.png"), dpi=150)
    plt.close()
    log.info("Grafico 1 salvo.")

    tem_selic = "selic" in anual.columns and anual["selic"].notna().any()
    cols_anual = ["ipca"] + (["selic"] if tem_selic else [])
    ult20 = anual[anual.index >= (anual.index.max() - 20)][cols_anual].dropna(subset=["ipca"])
    x = range(len(ult20))
    w = 0.4 if tem_selic else 0.6
    fig, ax = plt.subplots(figsize=(14, 5))
    if tem_selic:
        ax.bar([i - w/2 for i in x], ult20["ipca"], width=w, label="IPCA", color="#1f77b4")
        ax.bar([i + w/2 for i in x], ult20["selic"], width=w, label="Selic", color="#ff7f0e")
    else:
        ax.bar(list(x), ult20["ipca"], width=w, label="IPCA", color="#1f77b4")
    ax.set_xticks(list(x))
    ax.set_xticklabels(ult20.index, rotation=45, fontsize=8)
    ax.set_title("Media Anual: IPCA vs Selic (ultimos 20 anos)", fontsize=14, fontweight="bold")
    ax.set_ylabel("Media mensal (%)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "grafico2_anual_barras.png"), dpi=150)
    plt.close()
    log.info("Grafico 2 salvo.")

    fig, ax = plt.subplots(figsize=(7, 6))
    if tem_selic:
        dfsc = df.dropna(subset=["ipca", "selic"])
        sc = ax.scatter(dfsc["ipca"], dfsc["selic"], c=dfsc["ano"], cmap="plasma", alpha=0.5, s=12)
        plt.colorbar(sc, ax=ax, label="Ano")
        ax.set_title("Dispersao IPCA x Selic", fontsize=13, fontweight="bold")
        ax.set_xlabel("IPCA (%)")
        ax.set_ylabel("Selic (%)")
    else:
        dfsc = df.dropna(subset=["ipca"])
        decada = (dfsc["ano"] // 10) * 10
        sc = ax.scatter(dfsc["data"], dfsc["ipca"], c=decada, cmap="tab10", alpha=0.5, s=12)
        plt.colorbar(sc, ax=ax, label="Decada")
        ax.set_title("IPCA Mensal por Decada", fontsize=13, fontweight="bold")
        ax.set_xlabel("Data")
        ax.set_ylabel("IPCA (%)")
    plt.tight_layout()
    plt.savefig(os.path.join(DATA_DIR, "grafico3_dispersao.png"), dpi=150)
    plt.close()
    log.info("Grafico 3 salvo.")


def main():
    log.info("Iniciando pipeline...")

    raw_ipca = fetch_series("ipca", SERIES["ipca"])
    raw_selic = fetch_series("selic", SERIES["selic"])
    raw_cambio = fetch_series("cambio", SERIES["cambio"])

    df_ipca = processar_serie(raw_ipca, "ipca")
    df_selic = processar_serie(raw_selic, "selic")
    df_cambio = processar_serie(raw_cambio, "cambio")

    conn = conectar_mysql()
    criar_schema(conn)

    inserir_historico(conn, df_ipca, df_selic)
    inserir_cambio(conn, df_cambio)
    calcular_e_inserir_estatisticas(conn)

    df_mysql = consultar_dados(conn)
    anual = calcular_estatisticas_pandas(df_mysql)

    gerar_graficos(df_mysql, anual)

    conn.close()
    log.info("Pipeline finalizado.")


if __name__ == "__main__":
    main()