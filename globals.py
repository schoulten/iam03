# Bibliotecas ----
import pandas as pd


# Dados ----
pasta = "dados/"
df_mensal = pd.read_parquet(pasta + "df_mensal.parquet")
df_previsao = pd.read_parquet(pasta + "df_previsao.parquet")


# Objetos úteis ----
df_ipca = pd.concat(
    [
        (
            df_mensal
            .filter(["ipca"])
            .dropna()
            .rename(columns = {"ipca": "valor"})
            .assign(
                variavel = "IPCA",
                tipo = "Histórico"
            )
            .reset_index()
        ),
        (
            df_previsao
            .query("variavel == 'IPCA'")
        )
    ]
).sort_values("data")

modelos = df_previsao.groupby("variavel")["tipo"].unique().apply(list).to_dict()

df_tracking = (
    pd.read_csv(pasta + "tracking.csv")
    .set_index("data")
    .join(
        (
            df_mensal
            .filter(["ipca"])
            .dropna()
        ),
        how = "left"
    )
    .reset_index()
    .filter(["variavel", "previsao", "data", "tipo", "ipca", "valor"])
    .assign(
        erro = lambda x: x.ipca - x.valor
    )
    .rename(
        columns = {
            "variavel": "Variável",
            "previsao": "Data de Previsão",
            "data": "Horizonte da Previsão",
            "tipo": "Modelo",
            "ipca": "Observado",
            "valor": "Previsto",
            "erro": "Erro de Previsão"
        }
    )
    .round(2)
)

