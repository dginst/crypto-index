import pandas as pd
import numpy as np
import plotly.express as px
from pymongo import MongoClient
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

# connection = MongoClient("3.138.244.245", 27017)py
connection = MongoClient("localhost", 27017)


def query_mongo_x(database, collection, query_dict=None):

    # defining the variable that allows to work with MongoDB
    db = connection[database]
    coll = db[collection]
    if query_dict is None:

        df = pd.DataFrame(list(coll.find()))

        try:

            df = df.drop(columns="_id")

        except AttributeError:

            df = []

        except KeyError:

            df = []

    else:

        df = pd.DataFrame(list(coll.find(query_dict)))

        try:

            df = df.drop(columns="_id")

        except AttributeError:

            df = []

        except KeyError:

            df = []

    return df

# start app


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG],
                meta_tags=[{'name': 'viewport',
                            'content': 'width=device-width, initial-scale=1.0'}])
server = app.server

# -------------------
# Data
df = query_mongo_x("index", "index_level_1000")
df["Year"] = df['Date'].str[:4]
df = df.drop(columns="Time")
y_list = list(pd.unique(df["Year"]))
y_list = [int(y) for y in y_list]
df["Year"] = [int(y) for y in df["Year"]]

df_weight = query_mongo_x("index", "index_weights")
last_row_date = np.array(df_weight.tail(1)["Date"])[0]
date_list = np.array(df_weight["Date"])

df_weight = df_weight.drop(columns=["Time"])
df_no_time = df_weight.drop(columns="Date")


col_list = list(df_no_time.columns)


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

            dcc.RangeSlider(
                id="slct_years",
                marks={int(i): ' {}'.format(i) for i in y_list},
                min=y_list[0],
                max=y_list[len(y_list) - 1],
                value=[2017, 2018, 2019, 2020, 2021]
            ),

            dcc.Graph(id="my_index_level", figure={}),
        ])

    ]),

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


            dcc.Graph(id='my_weight_pie', figure={})
        ])

    ]),

])

# --------------------------
# Callbacks part

# crypto-index values graph


@app.callback(
    Output(component_id="my_index_level", component_property="figure"),
    Input(component_id="slct_years", component_property="value")
)
def update_graph(year_slct):

    dff = df.copy()
    dff_filtered = pd.DataFrame(columns=dff.columns)

    for y in year_slct:

        df_v = dff.loc[dff["Year"] == y]
        dff_filtered = dff_filtered.append(df_v)

    fig = px.line(
        data_frame=dff_filtered,
        x="Date",
        y="Index Value",
        template='plotly_dark'
    )

    return fig

# crypto-composition graph


@ app.callback(
    Output(component_id="my_weight_pie", component_property="figure"),
    Input(component_id="my_dropdown", component_property="value")
)
def update_pie(my_dropdown):

    dff_w = df_weight.copy()
    dff_w_filt = dff_w.loc[dff_w["Date"] == my_dropdown]

    dff_w_filt = dff_w_filt.drop(columns="Date")
    df_val = np.array(dff_w_filt)[0]
    df_col = list(dff_w_filt.columns)

    pie_fig = px.pie(
        # data_frame=dff_w_filt,
        values=df_val,
        names=df_col,
        # names=my_dropdown,
        hole=.3
    )

    return pie_fig


print("Done")
# --------------------
if __name__ == '__main__':
    app.run_server(debug=True, port=3000)
