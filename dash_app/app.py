from dash import Dash, html, dash_table, dcc, callback, Output, Input, ctx, no_update
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from pymongo import MongoClient
from dash_holoniq_wordcloud import DashWordcloud
import plotly.graph_objects as go
import numpy as np


client=MongoClient('mongo', port=27017)
performance={}
database="scrapping"
collection_sitemaps="sitemaps"
collection_htmls="htmls"


def match_keywords(client, database, collection_htmls, keywords, keywords_to_ignore, not_all_keywords_switch):

    list_match=list(filter(lambda  x:len(x), keywords.split(" ")))
    
    if not keywords_to_ignore:
        list_no_match=[]
    else:
        list_no_match=list(filter(lambda  x:len(x), keywords_to_ignore.split(" ")))


    if not not_all_keywords_switch:
        print("all element in list_match")
        return pd.DataFrame(list(client[database][collection_htmls].aggregate(
                                                        [
                                                            {
                                                                "$match" : 
                                                                {
                                                                    "$and":
                                                                        [
                                                                        {"mots_in_url": { "$all":  list_match }},
                                                                        {"mots_in_url": { "$nin":  list_no_match }}
                                                                        ]

                                                                },
                                                            },
                                                            { "$project": 
                                                                    { 
                                                                        "_id":0,
                                                                        "id_media": 1, 
                                                                        "media_name": 1, 
                                                                        "url" : 1, 
                                                                        "mots_in_url" :1,
                                                                        "xml_source" :1,
                                                                        "date_day":1,
                                                                        "date":1
                                                                    }
                                                            }
                                                        ]
                                                )
                                        )
        )
    else:
        print("one element atleast in list_match")
        return  pd.DataFrame(list(client[database][collection_htmls].aggregate(
                                                            [
                                                                {
                                                                    "$match" : 
                                                                    {
                                                                        "$and":
                                                                            [
                                                                            {"mots_in_url": { "$in":  list_match }},
                                                                            { "mots_in_url": { "$nin":  list_no_match }}
                                                                            ]

                                                                    },
                                                                },
                                                                { "$project": 
                                                                        { 
                                                                            "_id":0,
                                                                            "id_media": 1, 
                                                                            "media_name": 1, 
                                                                            "url" : 1, 
                                                                            "mots_in_url" :1,
                                                                            "xml_source" :1,
                                                                            "date_day":1,
                                                                            "date":1
                                                                        }
                                                                }
                                                            ]
                                                    )
                                            )
            )


# Initialize the app - incorporate a Dash Bootstrap theme
external_stylesheets = [dbc.themes.CERULEAN]
app = Dash(__name__, external_stylesheets=external_stylesheets)

# App layout
app.layout = dbc.Container([
        dbc.Row([
            html.Div('Medias Monitoring', className="text-primary text-center fs-3")
        ]),

        dbc.Row([
            dbc.Col([
                    html.Div([
                        dcc.Graph(id="medias_frequency", responsive=True),
                    ], className="center_horinzontal")
            ], width=6),

            dbc.Col([
                html.Div([],id="WordCloud_div", className="center_horinzontal")
            ], width=6),
        ]),

        dbc.Row([
            dbc.Col([
                    html.Div([
                        dcc.Graph(id="time_evolution"),
                    ], className="center_horinzontal")
            ], width={"size": 8, "offset":2}, align="center"),
        ]),

        dbc.Row([

            dbc.Col([
                        html.Div([
                                    dbc.Textarea(id="Keywords", size="lg", placeholder="Keywords", 
                                                 value='climate',),
                                ], className="center_horinzontal")
                            ], align="center"),

            dbc.Col([
                        html.Div([
                                    dbc.Textarea(id="Keywords_to_ignore", size="lg", placeholder="Keywords à enlever"),
                                ], className="center_horinzontal")
                            ], align="center"),
            

            dbc.Col([
                        html.Div([
                                    dbc.Textarea(id="Medias_to_ignore", size="lg", placeholder="Les médias à supprimer"),
                                ], className="center_horinzontal")
                            ], align="center"),

        ]),
        
        dbc.Row([
                dbc.Col([
                    html.Div(
                            [
                            dbc.Switch(
                                id="not_all_keywords_switch",
                                label="Intersection/Union",
                                value=False,
                                )
                            ], className="center_horinzontal")
                    ], width={"size": 4}, align="center")
                ,
                dbc.Col([
                        html.Div(
                                [
                                dbc.Button("Lancement de la requête", id="button_requete", color="primary", className="me-1 offset_vertical", size="lg")
                                ], className="center_horinzontal")
                        ], width={"size": 4}, align="center")
        ]),
    
    ], fluid=True)

def normalise(lst, vmax=50, vmin=16):
    lmax = max(lst, key=lambda x: x[1])[1]
    lmin = min(lst, key=lambda x: x[1])[1]
    vrange = vmax-vmin
    lrange = lmax-lmin or 1
    for entry in lst:
        entry[1] = int(((entry[1] - lmin) / lrange) * vrange + vmin)
    return lst


def wordcloud_graph(liste_frequence):
    return DashWordcloud(
                id='wordcloud',
                list=normalise(liste_frequence),
                color='blue',
                backgroundColor='#ffffff00',
                width=800, height=400,
                shuffle=False,
                rotateRatio=0.5,
                shrinkToFit=True,
                shape='circle',
                hover=True
                )

def graph_top_medias(df_freq):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_freq.loc[:, "media_name"],
        y=df_freq.loc[:, "count"],
    ))
    fig.update_layout(barmode='group', xaxis_tickangle=-45, margin=dict(l=20, r=20, t=20, b=20),) 
    return fig

def graph_top_moment(df_data):

    liste_remove=['Christian Science Monitor',
                    'Op Ed News',
                    'Washington City Paper',
                    'GW Hatchet',
                    'American Prospect',
                    'Diario Digital',
                    'Online Journal',
                    'Hollywood Weekly',
                    'American Free Press',
                    'American Free Press',
                    "American Independent",
                    'Southwester',
                    'MSN',
                    'CBS Sports',
                    'The Source',
                    'Access Hollywood',
                    'Associated Press',
                    'Guardian',
                    'Livingston Enterprise',
                    'NBC Sports',
                    'PBS News Hour',
                    'ESPN',
                    'Las Americas',
                    'Us',
                    'RTT News',
                    'Sanders County Ledger',
                    'Univision',
                    'The Conversation']

    df_data=df_data.loc[~df_data.loc[:, "media_name"].isin(liste_remove), :]
    df_freq=df_data.loc[:, "date_day"].dropna().value_counts().reset_index().sort_values(by="date_day")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_freq.loc[:, "date_day"],
        y=df_freq.loc[:, "count"],
    ))
    fig.update_layout(barmode='group', xaxis_tickangle=-45, width=800, height=200, margin=dict(l=20, r=20, t=20, b=20),) 
    return fig

# Add controls to build the interaction
@callback(
    Output(component_id='WordCloud_div', component_property='children'),
    Output(component_id='medias_frequency', component_property='figure'),
    Output(component_id='time_evolution', component_property='figure'),
    Input(component_id='button_requete', component_property='n_clicks'),
    Input(component_id='Keywords', component_property='value'),
    Input(component_id='Keywords_to_ignore', component_property='value'),
    Input(component_id='not_all_keywords_switch', component_property='value')
)
def update_wordcloud(click, keywords, keywords_to_ignore, not_all_keywords_switch):
    print(not_all_keywords_switch)
    if not ctx.triggered_id or ctx.triggered_id=="button_requete":
        df_data=match_keywords(client, database, collection_htmls, keywords, keywords_to_ignore, not_all_keywords_switch)
        n=200
        liste_parser=list(df_data.loc[:, "mots_in_url"].dropna())
        series_word_freq = pd.Series([ x for xs in liste_parser for x in xs if x not in keywords ]).value_counts().reset_index().iloc[:n]
        liste_frequence=list(map(lambda x,y:[x,y], series_word_freq.loc[:, "index"], series_word_freq.loc[:, "count"]))

        wordcloud=wordcloud_graph(liste_frequence)
        n_top=20
        most_frequent_medias=graph_top_medias(df_data.loc[:, "media_name"].dropna().value_counts().iloc[:n_top].reset_index())

        
        most_frequent_moment=graph_top_moment(df_data)
        return wordcloud, most_frequent_medias, most_frequent_moment
    else:
        return no_update

# Run the app
if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8000)