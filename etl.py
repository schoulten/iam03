# Pacotes ----
import pandas as pd
import numpy as np
import os
from utils import *


# Extração ----

# Importar metadados
df_metadados = pd.read_excel(
    io = "https://docs.google.com/spreadsheets/d/1NB1fkck-ol1y5fIETWDkoYG2sFhlgyKsADPfADK_98s/export?format=xlsx",
    sheet_name = "Metadados"
    )
df_metadados.head()

### BCB/SGS

# Filtra os códigos de API
codigos_bcb_sgs = (
    df_metadados
    .query("Fonte == 'BCB/SGS'")
    .reset_index(drop = True)[["Identificador", "Input de Coleta", "Frequência"]]
)

# Coleta dados do BCB/SGS
df_bruto_bcb_sgs = {
    "Diária": [],
    "Mensal": [],
    "Anual": []
}
for i in codigos_bcb_sgs.index:
  i_corrente = codigos_bcb_sgs.iloc[i]
  df_bruto_bcb_sgs[i_corrente["Frequência"]].append(
      coleta_bcb_sgs(
          codigo = i_corrente["Input de Coleta"],
          id = i_corrente["Identificador"],
          data_inicio = "01/01/2000",
          freq = i_corrente["Frequência"]
      )
  )

### BCB/ODATA

# Filtra os códigos de API
codigos_bcb_odata = (
    df_metadados
    .query("Fonte == 'BCB/ODATA'")
    .reset_index(drop = True)[["Identificador", "Input de Coleta"]]
    .set_index("Identificador")
    .to_dict()["Input de Coleta"]
)

# Coleta dados do BCB/ODATA
df_bruto_bcb_odata = codigos_bcb_odata.copy()
for id, url in codigos_bcb_odata.items():
  df_bruto_bcb_odata[id] = coleta_bcb_odata(url, id)

### IBGE/SIDRA

# Filtra os códigos de API
codigos_ibge_sidra = (
    df_metadados
    .query("Fonte == 'IBGE/SIDRA'")
    .reset_index(drop = True)[["Identificador", "Input de Coleta"]]
)

# Coleta dados do IBGE/SIDRA
df_bruto_ibge_sidra = []
for i in codigos_ibge_sidra.index:
  df_bruto_ibge_sidra.append(
      coleta_ibge_sidra(
          codigos_ibge_sidra.loc[i, "Input de Coleta"],
          codigos_ibge_sidra.loc[i, "Identificador"]
      )
    )

### IPEADATA

# Filtra os códigos de API
codigos_ipeadata = (
    df_metadados
    .query("Fonte == 'IPEADATA'")
    .reset_index(drop = True)[["Identificador", "Input de Coleta", "Frequência"]]
)

# Coleta dados do IPEADATA
df_bruto_ipeadata = {
    "Diária": [],
    "Mensal": []
}
for i in codigos_ipeadata.index:
  df_bruto_ipeadata[codigos_ipeadata.loc[i, "Frequência"]].append(
      coleta_ipeadata(
        codigos_ipeadata.loc[i, "Input de Coleta"],
        codigos_ipeadata.loc[i, "Identificador"]
      )
    )

### FRED

# Filtra os códigos de API
codigos_fred = (
    df_metadados
    .query("Fonte == 'FRED'")
    .reset_index(drop = True)[["Identificador", "Input de Coleta", "Frequência"]]
)

# Coleta dados do FRED
df_bruto_fred = {
    "Diária": [],
    "Mensal": []
}

for i in codigos_fred.index:
  df_bruto_fred[codigos_fred.loc[i, "Frequência"]].append(
      coleta_fred(
        codigos_fred.loc[i, "Input de Coleta"],
        codigos_fred.loc[i, "Identificador"]
      )
    )


# Transformação ----

### BCB/SGS

# Dados diárias
df_bruto_bcb_sgs_diaria = df_bruto_bcb_sgs["Diária"][0].join(df_bruto_bcb_sgs["Diária"][1:]).sort_index()

# Dados mensais
df_bruto_bcb_sgs_mensal = (
    df_bruto_bcb_sgs["Mensal"][0]
    .join(df_bruto_bcb_sgs["Mensal"][1:], how = "outer")
    .join(
        (
            df_bruto_bcb_sgs_diaria
            .resample("MS")
            .mean()
            .drop(["selic"], axis = "columns")
            .join(
                (
                    df_bruto_bcb_sgs_diaria
                    .assign(ano_mes = lambda x: x.index.to_period("M"))
                    .groupby("ano_mes")
                    .head(1)
                    .filter(["selic"])
                ),
                how = "outer"
            )
        ),
        how = "outer"
    )
    .join(
        pd.concat(
            [df_bruto_bcb_sgs["Anual"][0].resample("MS").ffill(),
            pd.DataFrame(
                data = {
                    "meta_inflacao": df_bruto_bcb_sgs["Anual"][0].iloc[-1].values
                },
                index = pd.date_range(
                  start = df_bruto_bcb_sgs["Anual"][0].index.max() + pd.DateOffset(months = 1),
                  end = df_bruto_bcb_sgs["Anual"][0].index.max() + pd.DateOffset(months = 11),
                  freq = "MS"
                  )
              )
            ]
        ),
        how = "outer"
    )
    .sort_index()
  )

### BCB/ODATA

# Filtra expectativas curto prazo ~1 mês à frente e agrega pela média
df_tratado_bcb_odata_ipca_cp = (
    df_bruto_bcb_odata["expec_ipca_top5_curto_prazo"]
    .assign(
        data = lambda x: pd.to_datetime(x.Data),
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%m/%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.data) / np.timedelta64(30, "D")).astype(int),
        expec_ipca_top5_curto_prazo = lambda x: x.Media
        )
    .query("horizonte == 1")
    .groupby(["DataReferencia"])["expec_ipca_top5_curto_prazo"]
    .mean()
    .sort_index()
    .rename_axis("data")
    .to_frame()
)

# Filtra expectativas médio prazo ~6 mês à frente e agrega pela média
df_tratado_bcb_odata_ipca_mp = (
    df_bruto_bcb_odata["expec_ipca_top5_medio_prazo"]
    .assign(
        data = lambda x: pd.to_datetime(x.Data),
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%m/%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.data) / np.timedelta64(30, "D")).astype(int),
        expec_ipca_top5_medio_prazo = lambda x: x.Media
        )
    .query("horizonte == 6")
    .groupby(["DataReferencia"])["expec_ipca_top5_medio_prazo"]
    .mean()
    .sort_index()
    .rename_axis("data")
    .to_frame()
)

# Filtra expectativas longo prazo ~1 ano à frente e agrega pela média
df_tratado_bcb_odata_selic = (
    df_bruto_bcb_odata["expec_selic"]
    .assign(
        data = lambda x: pd.to_datetime(x.Data).dt.to_period("M").dt.to_timestamp(),
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.data) / np.timedelta64(365, "D")).astype(int),
        expec_selic = lambda x: x.Media
        )
    .query("horizonte == 1")
    .groupby(["data"])["expec_selic"]
    .mean()
    .sort_index()
    .to_frame()
)

# Filtra expectativas curto prazo ~1 mês à frente e agrega pela média
df_tratado_bcb_odata_cambio = (
    df_bruto_bcb_odata["expec_cambio"]
    .assign(
        data = lambda x: pd.to_datetime(x.Data),
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%m/%Y"),
        horizonte = lambda x: ((x.DataReferencia - x.data) / np.timedelta64(30, "D")).astype(int),
        expec_cambio = lambda x: x.Media
        )
    .query("horizonte == 1")
    .groupby(["DataReferencia"])["expec_cambio"]
    .mean()
    .sort_index()
    .rename_axis("data")
    .to_frame()
)

# Filtra expectativas curto prazo ~12 meses à frente e agrega pela média
df_tratado_bcb_odata_ipca_lp = (
    df_bruto_bcb_odata["expec_ipca_12m"]
    .assign(
        data = lambda x: pd.to_datetime(x.Data).dt.to_period("M").dt.to_timestamp(),
        expec_ipca_12m = lambda x: x.Media
        )
    .groupby(["data"])["expec_ipca_12m"]
    .mean()
    .to_frame()
)

# Filtra expectativas médio prazo ~9 meses à frente e agrega pela média
df_tratado_bcb_odata_pib = (
    df_bruto_bcb_odata["expec_pib"]
    .assign(
        DataReferencia = lambda x: pd.PeriodIndex(
            x.DataReferencia.str.replace(r"(\d{1})/(\d{4})", r"\2-Q\1", regex = True),
            freq = "Q"
            ).to_timestamp(),
        data = lambda x: pd.to_datetime(x.Data).dt.to_period("Q").dt.to_timestamp(),
        horizonte = lambda x: ((x.DataReferencia - pd.to_datetime(x.Data)) / np.timedelta64(30, "D")).astype(int),
        expec_pib = lambda x: x.Media
      )
    .query("horizonte == 9")
    .groupby(["data"])["expec_pib"]
    .mean()
    .sort_index()
    .to_frame()
)

# Filtra expectativas longo prazo ~1 ano à frente e agrega pela média
df_tratado_bcb_odata_primario = (
    df_bruto_bcb_odata["expec_primario"]
    .assign(
        DataReferencia = lambda x: pd.to_datetime(x.DataReferencia, format = "%Y"),
        data = lambda x: pd.to_datetime(x.Data).dt.to_period("M").dt.to_timestamp(),
        horizonte = lambda x: ((x.DataReferencia - pd.to_datetime(x.Data)) / np.timedelta64(365, "D")).astype(int),
        expec_primario = lambda x: x.Media
        )
    .query("horizonte == 1")
    .groupby(["data"])["expec_primario"]
    .mean()
    .sort_index()
    .to_frame()
)

# Cruza dados de mesma frequência
df_tratado_bcb_odata_lista = [
    df_tratado_bcb_odata_ipca_mp,
    df_tratado_bcb_odata_ipca_lp,
    df_tratado_bcb_odata_selic,
    df_tratado_bcb_odata_cambio,
    df_tratado_bcb_odata_primario
  ]

df_tratado_bcb_odata_mensal = df_tratado_bcb_odata_ipca_cp.join(
    other = df_tratado_bcb_odata_lista,
    how = "outer"
    ).sort_index()

### IBGE/SIDRA

# Cruza dados
df_tratado_ibge_sidra = df_bruto_ibge_sidra[0].join(df_bruto_ibge_sidra[1:], how = "outer").sort_index()

### IPEADATA

# Trata dados diários
df_tratado_ipeadata_ipcs = (
    df_bruto_ipeadata["Diária"][0]
    .assign(ano_mes = lambda x: x.index.to_period("M"))
    .dropna()
    .groupby("ano_mes")
    .tail(1)
    .filter(["ipc_s"])
    .assign(data = lambda x: pd.to_datetime(x.index.strftime("%Y-%m-01")))
    .set_index("data")
)

# Trata dados mensais
df_tratado_ipeadata_mensal = (
    df_bruto_ipeadata["Mensal"][0]
    .join(df_bruto_ipeadata["Mensal"][1:], how = "outer")
    .join(df_tratado_ipeadata_ipcs, how = "outer")
    .sort_index()
)

### FRED

# Mensaliza séries diárias e concatena tabelas mensais
df_tratado_fred_mensal = (
    df_bruto_fred["Mensal"][0]
    .join(df_bruto_fred["Mensal"][1:], how = "outer")
    .join(pd.concat(df_bruto_fred["Diária"]).resample("MS").mean())
    .sort_index()
)

# Dados diários
df_tratado_fred_diario = pd.concat(df_bruto_fred["Diária"]).sort_index()


# Disponibilização de dados ----

# Caminho para salvar dados
PASTA_DRIVE = "dados/"
os.makedirs(PASTA_DRIVE, exist_ok = True)

# Dados diários
df_diaria = df_bruto_bcb_sgs_diaria.join(df_tratado_fred_diario, how = "outer").sort_index()
df_diaria.to_parquet(PASTA_DRIVE + "df_diaria.parquet")

# Dados mensais
df_mensal = (
    df_bruto_bcb_sgs_mensal
    .join(df_tratado_bcb_odata_mensal, how = "outer")
    .join(df_tratado_ibge_sidra, how = "outer")
    .join(df_tratado_ipeadata_mensal, how = "outer")
    .join(df_tratado_fred_mensal, how = "outer")
    .sort_index()
)

df_mensal.to_parquet(PASTA_DRIVE + "df_mensal.parquet")