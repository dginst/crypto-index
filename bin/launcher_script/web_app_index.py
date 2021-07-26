import urllib.parse

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from cryptoindex.config import CRYPTO_ASSET
from cryptoindex.mongo_setup import query_mongo
from dash.dependencies import Input, Output

# start app

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}])

app.css.append_css(
    {"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

server = app.server

# -------------------
# Important date lists definition

df_weight = query_mongo("index", "index_weights")

last_start_q = np.array(df_weight.tail(1)["Date"])[0]
start_q_list = np.array(df_weight["Date"])

# ----------------
# app layout: bootstrap

app.layout = dbc.Container([

    # create as much rows and columns as needed foe the dashboard
    dbc.Row([
        dbc.Col(html.H1("Crypto Index Dashboard",
                        className='text-center text-primary, mb-4'),
                width=12)
    ]),

    dbc.Row([

            dbc.Col([

                dbc.Card(
                    [
                        dbc.CardBody(
                            [

                                dbc.Row([

                                    dbc.Col([


                                        html.Label(['Mode:']),

                                        dcc.Dropdown(
                                            id='color_mode',
                                            options=[
                                                {'label': 'Light Mode',
                                                 'value': 'plotly_white'},
                                                {'label': 'Dark Mode',
                                                 'value': 'plotly_dark'}

                                            ],
                                            multi=False,
                                            value="plotly_dark",
                                            style={"width": "50%"},
                                            clearable=False
                                        ),
                                    ]),

                                ]),
                            ]),
                    ]),
            ]),
            ]),

    dbc.Row([
        dbc.Col([

            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            dbc.Row(
                                [
                                    dbc.Col([
                                        dcc.Graph(id="my_index_indicator", figure={},
                                                  config={'displayModeBar': False})
                                    ]),

                                    dbc.Col([
                                        dcc.Graph(id="my_index_today", figure={},
                                                  config={'displayModeBar': False})
                                    ])
                                ]),

                            html.Hr(),

                            dbc.Row(
                                [
                                    dbc.Col([


                                        dcc.Graph(id="my_index_level", figure={},
                                                  config={'displayModeBar': False}),

                                        html.A(
                                            'Download Data',
                                            id='download-link_index',
                                            download="index_level.csv",
                                            href='',
                                            target="_blank"
                                        ),

                                    ])
                                ]),

                        ]),
                ],
                style={"width": "70rem"},
                className="mt-3"
            )

        ]),

    ], justify='center'),

    dbc.Row([
            dbc.Col([

                dbc.Card(
                    [
                        dbc.CardBody(
                            [

                                dbc.Row([
                                    dbc.Col([

                                        html.Label(['Period']),

                                        dcc.Dropdown(
                                            id='my_dropdown',
                                            options=[
                                                {'label': x, 'value': x} for x in start_q_list

                                            ],
                                            multi=False,
                                            value=str(last_start_q),
                                            style={"width": "50%"},
                                            clearable=False
                                        ),

                                        dcc.Graph(
                                            id='my_weight_pie', figure={}),

                                        html.A(
                                            'Download Data',
                                            id='download-link_weight',
                                            download="index_weight.csv",
                                            href='',
                                            target="_blank"
                                        )
                                    ])
                                ]),

                            ]),
                    ],
                    style={"width": "70rem"},
                    className="mt-3"
                )

            ]),

            ], justify='center'),


    dbc.Row([
            dbc.Col([

                dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                dbc.Row([

                                    dbc.Col([

                                        html.Label(['Crypto Assets']),
                                        dcc.Checklist(
                                            id='my_crypto_check_2',
                                            options=[
                                                {'label': x, 'value': x} for x in CRYPTO_ASSET
                                            ],
                                            value=["BTC", "ETH",
                                                   "XRP", "LTC", "BCH"],
                                            labelStyle={
                                                'display': 'inline-block'},
                                            inputStyle={"margin-right": "10px",
                                                        "margin-left": "10px"}
                                        ),
                                        dcc.Graph(
                                            id="my_price_level", figure={}),

                                        html.A(
                                            'Download Data',
                                            id='download-link_price',
                                            download="crypto_price.csv",
                                            href='',
                                            target="_blank"
                                        )

                                    ])
                                ]),

                            ]),
                    ],
                    style={"width": "70rem"},
                    className="mt-3"
                )

            ]),

            ], justify='center'),

    dbc.Row([
            dbc.Col([

                dbc.Card(
                    [
                        dbc.CardBody(
                            [
                                dbc.Row([

                                    dbc.Col([
                                        html.Label(['Crypto Assets']),
                                        dcc.Checklist(
                                            id='my_crypto_check',
                                            options=[
                                                {'label': x, 'value': x} for x in CRYPTO_ASSET
                                            ],
                                            value=["BTC", "ETH",
                                                   "XRP", "LTC", "BCH"],
                                            labelStyle={
                                                'display': 'inline-block'},
                                            inputStyle={"margin-right": "10px",
                                                        "margin-left": "10px"}
                                        ),
                                        dcc.Graph(
                                            id="my_volume_level", figure={}),

                                        html.A(
                                            'Download Data',
                                            id='download-link_volume',
                                            download="crypto_volume.csv",
                                            href='',
                                            target="_blank"
                                        )
                                    ])
                                ]),

                            ]),
                    ],
                    style={"width": "70rem"},
                    className="mt-3"
                )

            ]),

            ], justify='center'),


    dcc.Interval(id='update', n_intervals=0, interval=1000 * 5),

    dcc.Interval(id='df-update', interval=100000, n_intervals=0)

])

# --------------------------
# Callbacks part


@app.callback(
    Output('my_index_level', 'figure'),
    [Input('df-update', 'n_intervals'),
     Input(component_id="color_mode", component_property="value")
     ]
)
def update_index_df(n, sel_col):

    df_index = query_mongo("index", "index_level_1000")

    dff = df_index.copy()
    dff = dff.loc[dff["Date"] > "2016-09-30"]
    dff_last = dff.tail(2)
    dff_y = dff_last[dff_last['Date']
                     == dff_last['Date'].min()]['Index Value'].values[0]
    dff_t = dff_last[dff_last['Date']
                     == dff_last['Date'].max()]['Index Value'].values[0]

    variation = (dff_t >= dff_y)
    dff["Var"] = variation

    index_area = px.area(
        data_frame=dff,
        x="Date",
        y="Index Value",
        # template='plotly_dark',
        template=sel_col,
        title='Crypto Index Level',
        color="Var",
        color_discrete_map={
            False: '#FD3216',
            True: '#1CA71C',

        }
    )
    index_area.update_layout(showlegend=False)

    return index_area


# index value part and elements


@ app.callback(
    [Output('my_index_indicator', 'figure'),
     Output('download-link_index', 'href')],
    Input('update', 'n_intervals')
)
def update_indicator(timer):

    df_index = query_mongo("index", "index_level_1000")

    dff_ind = df_index.copy()
    dff_last_ind = dff_ind.tail(2)
    dff_ind_y = dff_last_ind[dff_last_ind['Date']
                             == dff_last_ind['Date'].min()]['Index Value'].values[0]
    dff_ind_t = dff_last_ind[dff_last_ind['Date']
                             == dff_last_ind['Date'].max()]['Index Value'].values[0]

    fig_indicator = go.Figure(go.Indicator(
        mode="delta",
        value=dff_ind_t,
        delta={'reference': dff_ind_y, 'relative': True, 'valueformat': '.2%'}))
    fig_indicator.update_traces(delta_font={'size': 22})
    fig_indicator.update_layout(height=60, width=100)

    if dff_ind_t >= dff_ind_y:
        fig_indicator.update_traces(delta_increasing_color='green')
    elif dff_ind_t < dff_ind_y:
        fig_indicator.update_traces(delta_decreasing_color='red')

    csv_string_index = dff_ind.to_csv(index=False, encoding='utf-8')
    csv_string_index = "data:text/csv;charset=utf-8," + \
        urllib.parse.quote(csv_string_index)

    return fig_indicator, csv_string_index


@ app.callback(
    Output('my_index_today', 'figure'),
    Input('update', 'n_intervals')
)
def update_today_val(timer):

    df_index = query_mongo("index", "index_level_1000")

    dff_ind = df_index.copy()
    dff_last_ind = dff_ind.tail(1)
    dff_prev = (dff_ind.tail(2)).head(1)
    today_val = dff_last_ind["Index Value"].values[0]
    yesterday_val = dff_prev["Index Value"].values[0]

    fig_indicator = go.Figure(go.Indicator(
        mode="number+delta",
        value=today_val,
        delta={'relative': False,
               'reference': yesterday_val,
               'valueformat': ',.2f',
               'position': "right",
               },
        number={'suffix': "$",
                'valueformat': ',.2f',
                'font': {'color': 'black',
                         'size': 22}
                },
    )
    )
    fig_indicator.update_traces(delta_font={'size': 20})
    fig_indicator.update_layout(height=60,
                                width=300,
                                )

    return fig_indicator

# crypto-composition portfolio graph


@ app.callback(
    [Output(component_id="my_weight_pie", component_property="figure"),
     Output(component_id="download-link_weight", component_property="href")],
    [Input(component_id="my_dropdown", component_property="value"),
     Input(component_id="color_mode", component_property="value")]
)
def update_pie(my_dropdown, sel_col):

    df_weight = query_mongo("index", "index_weights")

    dff_weight = df_weight.copy()
    dff_weight = dff_weight.drop(columns="Time")
    dff_w_filt = dff_weight.loc[dff_weight["Date"] == my_dropdown]

    dff_w_filt = dff_w_filt.drop(columns="Date")

    df_col = list(dff_w_filt.columns)

    for col in df_col:

        val = np.array(dff_w_filt[col])[0]
        if val == 0.0000:

            dff_w_filt = dff_w_filt.drop(columns=col)

    df_val = np.array(dff_w_filt)[0]
    df_col_2 = list(dff_w_filt.columns)

    pie_fig = px.pie(
        values=df_val,
        names=df_col_2,
        hole=.3,
        template=sel_col,
        title='Index Weights',
        color=df_col_2,
        color_discrete_map={
            "BTC": "#FEAF16",
            "ETH": "#511CFB",
            "XRP": "#F6222E",
            "LTC": "#E2E2E2",
            "BCH": "#86CE00",
            "EOS": "#FBE426",
            "ETC": "#DA16FF",
            "ZEC": "#B68100",
            "ADA": "#00B5F7",
            "XLM": "#750D86",
            "XMR": "#A777F1",
            "BSV": "#F58518"
        }
    )

    csv_string_weight = dff_weight.to_csv(index=False, encoding='utf-8')
    csv_string_weight = "data:text/csv;charset=utf-8," + \
        urllib.parse.quote(csv_string_weight)

    return pie_fig, csv_string_weight


@ app.callback(
    [Output(component_id="my_price_level", component_property="figure"),
     Output(component_id="download-link_price", component_property="href")],
    [Input(component_id="my_crypto_check_2", component_property="value"),
     Input(component_id="color_mode", component_property="value")]
)
def update_price(my_checklist, sel_col):

    df_price = query_mongo("index", "crypto_price")

    dff_price = df_price.copy()
    dff_date = dff_price["Date"]
    dff_price_filtered = dff_price[my_checklist]
    dff_price_filtered["Date"] = dff_date

    price_line = px.line(
        data_frame=dff_price_filtered,
        x="Date",
        y=my_checklist,
        template=sel_col,
        title='Crypto Prices',
        labels={"value": "Price (USD)",
                "variable": ""},
        color_discrete_map={
            "BTC": "#FEAF16",
            "ETH": "#511CFB",
            "XRP": "#F6222E",
            "LTC": "#E2E2E2",
            "BCH": "#86CE00",
            "EOS": "#FBE426",
            "ETC": "#DA16FF",
            "ZEC": "#B68100",
            "ADA": "#00B5F7",
            "XLM": "#750D86",
            "XMR": "#A777F1",
            "BSV": "#F58518"
        }
    )

    dff_price_d = df_price.copy()
    dff_price_d = dff_price_d.drop(columns="Time")
    csv_string_price = dff_price_d.to_csv(index=False, encoding='utf-8')
    csv_string_price = "data:text/csv;charset=utf-8," + \
        urllib.parse.quote(csv_string_price)

    return price_line, csv_string_price


@ app.callback(
    [Output(component_id="my_volume_level", component_property="figure"),
     Output(component_id="download-link_volume", component_property="href")],
    [Input(component_id="my_crypto_check", component_property="value"),
     Input(component_id="color_mode", component_property="value")]
)
def update_vol(my_checklist, sel_col):

    df_volume = query_mongo("index", "crypto_volume")

    dff_vol = df_volume.copy()
    dff_date = dff_vol["Date"]
    dff_vol_filtered = dff_vol[my_checklist]
    dff_vol_filtered["Date"] = dff_date

    volume_line = px.line(
        data_frame=dff_vol_filtered,
        x="Date",
        y=my_checklist,
        template=sel_col,
        title='Crypto Volumes',
        labels={"value": "Volume (USD)",
                "variable": ""},
        color_discrete_map={
            "BTC": "#FEAF16",
            "ETH": "#511CFB",
            "XRP": "#F6222E",
            "LTC": "#E2E2E2",
            "BCH": "#86CE00",
            "EOS": "#FBE426",
            "ETC": "#DA16FF",
            "ZEC": "#B68100",
            "ADA": "#00B5F7",
            "XLM": "#750D86",
            "XMR": "#A777F1",
            "BSV": "#F58518"
        }
    )

    dff_volume = df_volume.copy()
    dff_volume = dff_volume.drop(columns="Time")
    csv_string_volume = dff_volume.to_csv(index=False, encoding='utf-8')
    csv_string_volume = "data:text/csv;charset=utf-8," + \
        urllib.parse.quote(csv_string_volume)

    return volume_line, csv_string_volume


print("Done")
# --------------------
if __name__ == '__main__':
    app.run_server(debug=False, port=3000, host='0.0.0.0')
