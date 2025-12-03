from shiny import Inputs, Outputs, Session, render, ui, reactive
from globals import df_ipca, df_tracking
from faicons import icon_svg
from shinywidgets import render_plotly
import plotly.express as px
import pandas as pd

def server(input: Inputs, output: Outputs, session: Session):

    @reactive.calc
    def obter_ano_previsao_atual():
        modelo_selecionado = input.modelos()
        ano_previsao = (
            df_ipca
            .query("tipo in @modelo_selecionado")
            .dropna(subset = "valor")
            .data
            .min()
            .year
        )
        return ano_previsao
    
    @reactive.calc
    def calc_yoy_de_mom():
        ano_previsao = obter_ano_previsao_atual()
        modelo_selecionado = input.modelos()
        yoy = (
            df_ipca
            .query("data.dt.year == @ano_previsao and tipo in ['Histórico', @modelo_selecionado]")
            .assign(
                valor_yoy = lambda x: x.valor.rolling(window = 12).apply(
                    lambda x: ((x / 100 + 1).prod() - 1) * 100, 
                    raw = False
                ).round(2)
            )
            .query("data == data.max()")
            .valor_yoy
            .iloc[0]
        )
        return yoy
    
    @reactive.calc
    def ultima_previsao_mom():
        modelo_selecionado = input.modelos()
        mom = (
            df_ipca
            .query("tipo == @modelo_selecionado")
            .query("data == data.min()")
            .filter(["data", "valor"])
            .round(2)
        )
        return mom
    
    @reactive.calc
    def ultimo_valor_historico():
        ult = (
            df_ipca
            .query("tipo == 'Histórico' and variavel == 'IPCA'")
            .query("data == data.max()")
            .filter(["data", "valor"])
            .round(2)
        )
        return ult
    
    @reactive.calc
    def obter_dados_fanchart():
        modelo_selecionado = input.modelos()
        df_fanchart = (
            df_ipca
            .query("variavel == 'IPCA' and tipo in ['Histórico', @modelo_selecionado]")
            .filter(["data", "valor", "tipo", "ic_inferior", "ic_superior"])
            .assign(
                data = lambda x: x.data.dt.strftime("%Y-%m-%d"),
                tipo = lambda x: x.tipo.str.replace("Histórico", "IPCA")
                )
            .round(2)
        )
        return df_fanchart

    @reactive.calc
    def obter_dados_fantable():
        df_fantable = (
            obter_dados_fanchart()
            .dropna(subset = ["ic_inferior", "ic_superior"])
            .assign(data = lambda x: pd.to_datetime(x.data).dt.strftime("%m/%Y"))
            .filter(["data", "ic_inferior", "valor", "ic_superior"])
            .rename(
                columns = {
                    "data": "Data",
                    "ic_inferior": "IC Inferior",
                    "valor": "Previsão (%)",
                    "ic_superior": "IC Superior"
                }
            )
        )
        return df_fantable

    @render.ui
    def card_yoy():
        ano_previsao = obter_ano_previsao_atual()
        valor_yoy = calc_yoy_de_mom()
        return ui.value_box(
            f"Previsão {ano_previsao}",
            f"{valor_yoy}%",
            "YoY",
            showcase = icon_svg("calendar")
        )
    
    @render.ui
    def card_mom():
        mom = ultima_previsao_mom()
        data = mom.data.dt.strftime("%m/%Y").iloc[0]
        valor_mom = mom.valor.iloc[0]
        return ui.value_box(
            f"Previsão {data}",
            f"{valor_mom}%",
            "MoM",
            showcase = icon_svg("percent")
        )
    
    @render.ui
    def card_last():
        ult = ultimo_valor_historico()
        data = ult.data.dt.strftime("%m/%Y").iloc[0]
        valor_mom = ult.valor.iloc[0]
        return ui.value_box(
            f"Realizado {data}",
            f"{valor_mom}%",
            "MoM",
            showcase = icon_svg("magnifying-glass-chart")
        )
    
    @render_plotly
    def fanchart():
        df_fanchart = obter_dados_fanchart()
        fig = px.line(
            data_frame = df_fanchart.tail(12*20),
            x = "data",
            y = "valor",
            color = "tipo",
            title = "Previsão do IPCA",
            labels = {"data": "Data", "valor": "Valor (%)", "tipo": "Série"},
            hover_data = {"data": "|%d/%m/%Y", "valor": ":.2f"}
        )

        fig.update_layout(
            legend = dict(
                orientation = "h",
                yanchor = "bottom",
                y = -0.3,
                xanchor = "center",
                x = 0.5
            )
        )

        df_ic = df_fanchart.dropna(subset = ["ic_inferior", "ic_superior"])
        fig.add_traces([
            dict(
                x = df_ic["data"],
                y = df_ic["ic_inferior"],
                mode = "lines",
                line = dict(width = 0),
                showlegend = False,
                name = "IC Inferior"
            ),
            dict(
                x = df_ic["data"],
                y = df_ic["ic_superior"],
                mode = "lines",
                line = dict(width = 0),
                fill = "tonexty",
                fillcolor = "rgba(0,100,255,0.2)",
                showlegend = True,
                name = "Intervalo de Confiança"
            )
        ])

        return fig
    
    @render.data_frame
    def fantable():
        return obter_dados_fantable()
    
    @render.data_frame
    def tracking():
        modelo_selecionado = input.modelos()
        return render.DataGrid(df_tracking.query("Modelo == @modelo_selecionado"), summary = False)