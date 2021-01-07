from cryptoindex.mongo_setup import (
    query_mongo
)
import pandas as pd
import plotly.express as px

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output


# start app

app = dash.Dash(__name__)
# server = app.server
# -------------------
# Data
df = query_mongo("index", "index_level_1000")
print(df)
df["Year"] = df['Date'].str[:4]
df = df.drop(columns="Time")
y_list = list(pd.unique(df["Year"]))
y_list = [int(y) for y in y_list]
df["Year"] = [int(y) for y in df["Year"]]
# ----------------
# app layout

app.layout = html.Div([

    html.H1("Crypto Index", style={"text-align": "center"}),

    dcc.RangeSlider(
        id="slct_years",
        marks={int(i): ' {}'.format(i) for i in y_list},
        min=y_list[0],
        max=y_list[len(y_list) - 1],
        value=[2017, 2018, 2019, 2020, 2021]
    ),

    html.Div(id="out_cont", children=[]),

    html.Br(),


    dcc.Graph(id="my_index_level", figure={})
])


# --------------------
# connect the plotly graph with Dash comp

@app.callback(
    [
        Output(component_id="out_cont", component_property="children"),
        Output(component_id="my_index_level", component_property="figure")
    ],
    [Input(component_id="slct_years", component_property="value")]
)
def update_graph(option_slct):

    print(option_slct)
    print(type(option_slct))

    container = "The selected years are: {}".format(option_slct)

    dff = df.copy()
    dff_filtered = pd.DataFrame(columns=dff.columns)
    for y in option_slct:

        df_v = dff.loc[dff["Year"] == y]
        dff_filtered = dff_filtered.append(df_v)

    print(dff_filtered)

    fig = px.line(
        data_frame=dff_filtered,
        x="Date",
        y="Index Value",
        template='plotly_dark'

    )

    print("H")
    return container, fig


# --------------------
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
