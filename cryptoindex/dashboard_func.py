from threading import Thread
import time
import numpy as np
import pandas as pd
import plotly.express as px
from cryptoindex.mongo_setup import (
    query_mongo
)
from cryptoindex.global_variable import (
    df_index, df_price, df_volume, df_weight,
    last_start_q, start_q_list, index_area_fig
)


class web_app_data(Thread):

    def __init__(self, nome, durata):
        Thread.__init__(self)
        self.nome = nome
        self.durata = durata

    def run(self):

        i = 1

        # global variables
        global df_index
        global df_price
        global df_volume
        global df_weight
        global last_start_q
        global start_q_list
        global index_area_fig

        while i < 2:

            print("Thread '" + self.name + "' avviato")

            df_index = query_mongo("index", "index_level_1000")

            # index_area_fig = index_area_chart_update(df_index)

            df_weight = query_mongo("index", "index_weights")

            df_volume = query_mongo("index", "crypto_volume")

            df_price = query_mongo("index", "crypto_price")

            last_start_q = np.array(df_weight.tail(1)["Date"])[0]
            start_q_list = np.array(df_weight["Date"])

            time.sleep(self.durata)
            print("Thread '" + self.name + "' terminato")


def index_area_df_definition(index_df):

    dff = index_df.copy()
    dff = dff.loc[dff["Date"] > "2016-09-30"]
    dff_last = dff.tail(2)
    dff_y = dff_last[dff_last['Date']
                     == dff_last['Date'].min()]['Index Value'].values[0]
    dff_t = dff_last[dff_last['Date']
                     == dff_last['Date'].max()]['Index Value'].values[0]

    variation = (dff_t >= dff_y)
    dff["Var"] = variation

    return dff


def index_area_figure(index_df):

    index_area_fig = px.area(
        data_frame=index_df,
        x="Date",
        y="Index Value",
        template='plotly_dark',
        title='Crypto Index Level',
        color="Var",
        color_discrete_map={
            False: '#FD3216',
            True: '#1CA71C',

        }
    )

    index_area_fig.update_layout(showlegend=False)

    return index_area_fig


def index_area_chart_update(index_df):

    index_dff = index_area_df_definition(index_df)

    index_area_fig = index_area_figure(index_dff)

    return index_area_fig
