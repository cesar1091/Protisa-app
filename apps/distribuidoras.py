import pandas as pd
import streamlit as st
from PIL import Image
from model_functions_data import process

# APP
def app():
    
    diccionario = {'Cantidad':'Cantidad','Soles':'Soles','Toneladas':'Toneladas'}

    # Portada
    image = Image.open('apps/BANNER-PROTISA.jpg')
    st.image(image, caption='Project test')
    level_data = st.sidebar.radio('Seleccione el nivel:',('Cantidad','Soles','Toneladas'))
    if level_data == 'Cantidad':
        # DISTRIBUIDOR
        dataset_dist = pd.read_csv("apps/dataset/Cantidad/distribuidor.csv",parse_dates=['Fecha'])
        # FILTRAR POR DISTRIBUIDOR
        dist = list(dataset_dist.Distribuidor.unique())
        # NIVEL LOCALIDAD
        dataset_loc = pd.read_csv("apps/dataset/Cantidad/localidad.csv",parse_dates=['Fecha'])
        # FILTRAR POR LOCALIDAD
        localidades = list(dataset_loc.Localidad.unique())
        # NIVEL REGION
        dataset_reg = pd.read_csv("apps/dataset/Cantidad/region.csv",parse_dates=['Fecha'])
        ##FILTRAR POR REGION
        regiones = list(dataset_reg.Region.unique())
    
    elif level_data == 'Soles':
        # DISTRIBUIDOR
        dataset_dist = pd.read_csv("apps/dataset/Soles/distribuidor.csv",parse_dates=['Fecha'])
        # FILTRAR POR DISTRIBUIDOR
        dist = list(dataset_dist.Distribuidor.unique())
        # NIVEL LOCALIDAD
        dataset_loc = pd.read_csv("apps/dataset/Soles/localidad.csv",parse_dates=['Fecha'])
        # FILTRAR POR LOCALIDAD
        localidades = list(dataset_loc.Localidad.unique())
        # NIVEL REGION
        dataset_reg = pd.read_csv("apps/dataset/Soles/region.csv",parse_dates=['Fecha'])
        ##FILTRAR POR REGION
        regiones = list(dataset_reg.Region.unique())
    
    elif level_data == 'Toneladas':
       # DISTRIBUIDOR
        dataset_dist = pd.read_csv("apps/dataset/Toneladas/distribuidor.csv",parse_dates=['Fecha'])
        # FILTRAR POR DISTRIBUIDOR
        dist = list(dataset_dist.Distribuidor.unique())
        # NIVEL LOCALIDAD
        dataset_loc = pd.read_csv("apps/dataset/Toneladas/localidad.csv",parse_dates=['Fecha'])
        # FILTRAR POR LOCALIDAD
        localidades = list(dataset_loc.Localidad.unique())
        # NIVEL REGION
        dataset_reg = pd.read_csv("apps/dataset/Toneladas/region.csv",parse_dates=['Fecha'])
        ##FILTRAR POR REGION
        regiones = list(dataset_reg.Region.unique())

    level = st.sidebar.radio('Selecciona el nivel:',('Distribuidor','Localidad','Region'))

    if level == 'Distribuidor':
        dataset = dataset_dist
        dist_select = st.sidebar.multiselect('Selecciona los productos:', dist)
        for i in dist_select:
            process(dataset,diccionario,level_data,level,i,index_col="Fecha")

    elif level == 'Localidad':
        dataset = dataset_loc
        loc_select = st.sidebar.multiselect('Selecciona las marcas:',localidades)
        for i in loc_select:
            process(dataset,diccionario,level_data,level,i,index_col="Fecha")

    else:
        dataset = dataset_reg
        cat_select = st.sidebar.multiselect('Selecciona las categorias:', regiones)
        for i in cat_select:
            process(dataset,diccionario,level_data,level,i,index_col="Fecha")