import pandas as pd
import numpy as np
import streamlit as st
from plotly.subplots import make_subplots
import datetime
import xgboost
from pycaret.regression import load_model, predict_model

def cumsell(df,level):
        df['Month'] = df.index.month
        df['Weekday'] = df.index.weekday
        df['Year'] = df.index.year
        df['CumSumByMonth'] = df.groupby(['Year','Month'])[level].cumsum().shift(1)
        df.dropna(inplace=True)
        df.drop(['Year','Month'],axis=1,inplace=True)
        return df

def lag_sells(df,level):
        lista = []
        for lag in range(5,31,5):
            df[f'LAG_{lag}'] = df[level].shift(lag)
            lista.append(f'LAG_{lag}')
        var_num = ['LAG_5','LAG_10','LAG_15','LAG_20','LAG_25','LAG_30','CumSumByMonth']
        for k in lista:
            for j in range(1,5):
                df[f'{k}-pct-change-{j}'] = df[k].pct_change(periods=j)
                df[f'{k}-diff-{j}'] = df[k].diff(periods=j)
                var_num.append(f'{k}-pct-change-{j}')
                var_num.append(f'{k}-diff-{j}')
        df.dropna(inplace=True)
        return df, var_num

def process(dataset,diccionario,level_data,level,i,index_col="Fecha"):
    df = dataset[dataset[f'{level}'] == f'{i}'].drop(f'{level}',axis=1).set_index(f'{index_col}')[[diccionario[level_data]]]
    df = df.resample('B').sum()
    df = df.interpolate()
    a = pd.DataFrame([0.01],columns=[diccionario[level_data]])
    a.index = df.tail(1).index + datetime.timedelta(days=1)
    df = df.append(a)
    df = np.abs(df)
    df = np.log(df)
    df = cumsell(df, diccionario[level_data])
    df.dropna(inplace=True)
    df, var_num = lag_sells(df, diccionario[level_data])
    score_df = df.drop(diccionario[level_data],axis=1).iloc[-100:]
    l = load_model(f'apps/trained_models/{level_data}/{level}/' + str(i.replace(' ','').replace('/','_')), verbose=False)
    p = predict_model(l, data=score_df)
    p['Label'] = np.exp(p['Label'])
    df[diccionario[level_data]] = np.exp(df[diccionario[level_data]])
    data = pd.concat([df[[diccionario[level_data]]],p[['Label']]],axis=1)
    data_line_chart = data.reset_index()
    data_line_chart.columns = ['Fecha',f'{level_data}','Prediccion']
    import plotly.graph_objects as go
    fig = make_subplots(
        rows=2, cols=1,
        horizontal_spacing = 0.1,
        row_titles = ["Forecast plot","Residual plot"]
    )

    p['Residual'] = data_line_chart[f'{level_data}'].iloc[-100:].values - p['Label'].values
    fig.add_trace(go.Scatter(x=data_line_chart['Fecha'],y=data_line_chart[f'{level_data}'],mode='lines',name='Real Values'),row=1,col=1)
    fig.add_trace(go.Scatter(x=data_line_chart['Fecha'],y=data_line_chart['Prediccion'],mode='lines',name='Predict Values'),row=1,col=1)
    fig.add_trace(go.Scatter(x=p['Label'],y=p['Residual'],mode='markers',name="Residuals"),row=2,col=1)
    #fig = px.line(data_line_chart, x='Fecha', y=[f'{level_data}', 'Prediccion'], title=f'Cierre diario - {i}', template = 'plotly_dark')
    st.plotly_chart(fig)
    prediccion_today = data.tail(4).round(2)
    prediccion_today['Prediccion'] = prediccion_today['Label']
    #prediccion_today['Fecha'] = fecha
    with st.beta_expander("See prediction for today"):
        st.table(prediccion_today[[diccionario[level_data],'Prediccion']])