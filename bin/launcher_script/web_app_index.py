import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import urllib.parse
from cryptoindex.mongo_setup import (
    query_mongo
)

# start app


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}])

app.css.append_css(
    {"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})

server = app.server

# -------------------
# Data
df = query_mongo("index", "index_level_1000")
df["Year"] = df['Date'].str[:4]
df = df.drop(columns="Time")
y_list = list(pd.unique(df["Year"]))
y_list = [int(y) for y in y_list]
df["Year"] = [int(y) for y in df["Year"]]

df_weight = query_mongo("index", "index_weights")
last_row_date = np.array(df_weight.tail(1)["Date"])[0]
date_list = np.array(df_weight["Date"])

df_weight = df_weight.drop(columns=["Time"])
df_no_time = df_weight.drop(columns="Date")

col_list = list(df_no_time.columns)

df_volume = query_mongo("index", "crypto_volume")

df_price = query_mongo("index", "crypto_price")

# ----------------------
# index level graph and link to download

dff = df.copy()
dff = dff.loc[dff["Date"] > "2016-09-30"]
dff_last = dff.tail(2)
dff_y = dff_last[dff_last['Date']
                 == dff_last['Date'].min()]['Index Value'].values[0]
dff_t = dff_last[dff_last['Date']
                 == dff_last['Date'].max()]['Index Value'].values[0]


variation = (dff_t >= dff_y)
dff["Var"] = variation

# index_line = px.line(
#     data_frame=df,
#     x="Date",
#     y="Index Value",
#     template='plotly_dark',
#     title='Crypto Index Level')

index_area = px.area(
    data_frame=dff,
    x="Date",
    y="Index Value",
    template='plotly_dark',
    title='Crypto Index Level',
    color="Var",
    color_discrete_map={
        True: '#1CA71C',
        False: '#00FE35'
    }
)
index_area.update_layout(showlegend=False)

dff_index = df.copy()
dff_index = dff_index.drop(columns="Year")
csv_string_index = df.to_csv(index=False, encoding='utf-8')
csv_string_index = "data:text/csv;charset=utf-8," + \
    urllib.parse.quote(csv_string_index)

# weights link to download

dff_weights = df_weight.copy()
csv_string_weight = dff_weights.to_csv(index=False, encoding='utf-8')
csv_string_weight = "data:text/csv;charset=utf-8," + \
    urllib.parse.quote(csv_string_weight)

# crypto prices link to download

dff_prices = df_price.copy()
dff_prices = dff_prices.drop(columns="Time")
csv_string_price = dff_prices.to_csv(index=False, encoding='utf-8')
csv_string_price = "data:text/csv;charset=utf-8," + \
    urllib.parse.quote(csv_string_price)

# crypto volumes link to download

dff_volume = df_volume.copy()
dff_volume = dff_volume.drop(columns="Time")
csv_string_volume = dff_volume.to_csv(index=False, encoding='utf-8')
csv_string_volume = "data:text/csv;charset=utf-8," + \
    urllib.parse.quote(csv_string_volume)

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
                            dbc.Row(
                                [
                                    dbc.Col([
                                        dcc.Graph(id="my_index_indicator", figure={},
                                                  config={'displayModeBar': False})
                                    ])
                                ]),

                            dbc.Row(
                                [
                                    dbc.Col([

                                        dcc.Graph(id="my_index_level", figure=index_area,
                                                  config={'displayModeBar': False}),

                                        html.A(
                                            'Download Data',
                                            id='download-link_index',
                                            download="index_level.csv",
                                            href=csv_string_index,
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

            html.Label(['Period']),

            dcc.Dropdown(
                id='my_dropdown',
                options=[
                    {'label': x, 'value': x} for x in date_list
                ],
                multi=False,
                value=str(last_row_date),
                style={"width": "50%"},
                clearable=False
            ),

            dcc.Graph(id='my_weight_pie', figure={}),

            html.A(
                'Download Data',
                id='download-link_weight',
                download="index_weight.csv",
                href=csv_string_weight,
                target="_blank"
            )
        ])
    ]),

    dbc.Row([
            dbc.Col([

                html.Label(['Crypto Assets']),
                dcc.Checklist(
                    id='my_crypto_check',
                    options=[
                        {'label': x, 'value': x} for x in col_list
                    ],
                    value=["BTC", "ETH", "XRP", "LTC", "BCH"],
                    labelStyle={'display': 'inline-block'},
                    inputStyle={"margin-right": "10px",
                                "margin-left": "10px"}
                ),
                dcc.Graph(id="my_volume_level", figure={}),

                html.A(
                    'Download Data',
                    id='download-link_volume',
                    download="crypto_volume.csv",
                    href=csv_string_volume,
                    target="_blank"
                )
            ]),
            dbc.Col([

                html.Label(['Crypto Assets']),
                dcc.Checklist(
                    id='my_crypto_check_2',
                    options=[
                        {'label': x, 'value': x} for x in col_list
                    ],
                    value=["BTC", "ETH", "XRP", "LTC", "BCH"],
                    labelStyle={'display': 'inline-block'},
                    inputStyle={"margin-right": "10px",
                                "margin-left": "10px"}
                ),
                dcc.Graph(id="my_price_level", figure={}),

                html.A(
                    'Download Data',
                    id='download-link_price',
                    download="crypto_price.csv",
                    href=csv_string_price,
                    target="_blank"
                )
            ])
            ]),

    dcc.Interval(id='update', n_intervals=0, interval=1000 * 5)

])

# --------------------------
# Callbacks part

# index value part and elements


@ app.callback(
    Output('my_index_indicator', 'figure'),
    Input('update', 'n_intervals')
)
def update_indicator(timer):

    dff_ind = df.copy()
    dff_last_ind = dff_ind.tail(2)
    dff_ind_y = dff_last_ind[dff_last_ind['Date']
                             == dff_last_ind['Date'].min()]['Index Value'].values[0]
    dff_ind_t = dff_last_ind[dff_last_ind['Date']
                             == dff_last_ind['Date'].max()]['Index Value'].values[0]

    fig_indicator = go.Figure(go.Indicator(
        mode="delta",
        value=dff_t,
        delta={'reference': dff_ind_y, 'relative': True, 'valueformat': '.2%'}))
    fig_indicator.update_traces(delta_font={'size': 12})
    fig_indicator.update_layout(height=30, width=70)

    if dff_ind_t >= dff_ind_y:
        fig_indicator.update_traces(delta_increasing_color='green')
    elif dff_ind_t < dff_ind_y:
        fig_indicator.update_traces(delta_decreasing_color='red')

    return fig_indicator


# crypto-composition portfolio graph


@ app.callback(
    Output(component_id="my_weight_pie", component_property="figure"),
    Input(component_id="my_dropdown", component_property="value")
)
def update_pie(my_dropdown):

    dff_w = df_weight.copy()
    dff_w_filt = dff_w.loc[dff_w["Date"] == my_dropdown]

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
        template='plotly_dark',
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

    return pie_fig


@ app.callback(
    Output(component_id="my_price_level", component_property="figure"),
    Input(component_id="my_crypto_check_2", component_property="value")
)
def update_price(my_checklist):

    df_price = query_mongo("index", "crypto_price")

    dff_price = df_price.copy()
    dff_date = dff_price["Date"]
    dff_filtered = dff_price[my_checklist]
    dff_filtered["Date"] = dff_date

    price_line = px.line(
        data_frame=dff_filtered,
        x="Date",
        y=my_checklist,
        template='plotly_dark',
        title='Crypto Prices',
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

    return price_line


@ app.callback(
    Output(component_id="my_volume_level", component_property="figure"),
    Input(component_id="my_crypto_check", component_property="value")
)
def update_vol(my_checklist):

    dff_vol = df_volume.copy()
    dff_date = dff_vol["Date"]
    dff_filtered = dff_vol[my_checklist]
    dff_filtered["Date"] = dff_date

    volume_line = px.line(
        data_frame=dff_filtered,
        x="Date",
        y=my_checklist,
        template='plotly_dark',
        title='Crypto Volumes',
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

    return volume_line


print("Done")
# --------------------
if __name__ == '__main__':
    app.run_server(debug=True, port=3000, host='0.0.0.0')
