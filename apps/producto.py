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
        # SKU
        dataset_sku = pd.read_csv("apps/dataset/Cantidad/sku.csv",parse_dates=['Fecha'])
        # FILTRAR POR SKU
        skus = list(dataset_sku.SKU.unique())
        # NIVEL MARCA
        dataset_marca = pd.read_csv("apps/dataset/Cantidad/marca.csv",parse_dates=['Fecha'])
        # FILTRAR POR MARCA
        marcas_prod = list(dataset_marca.Marca.unique())
        # NIVEL CATEGORIA
        dataset_cat = pd.read_csv("apps/dataset/Cantidad/categoria.csv",parse_dates=['Fecha'])
        ##FILTRAR POR CATEGORIA
        categorias = list(dataset_cat.Categoria.unique())
    
    elif level_data == 'Soles':
        # SKU
        dataset_sku = pd.read_csv("apps/dataset/Soles/sku.csv",parse_dates=['Fecha'])
        # FILTRAR POR SKU
        skus = list(dataset_sku.SKU.unique())
        # NIVEL MARCA
        dataset_marca = pd.read_csv("apps/dataset/Soles/marca.csv",parse_dates=['Fecha'])
        # FILTRAR POR MARCA
        marcas_prod = list(dataset_marca.Marca.unique())
        # NIVEL CATEGORIA
        dataset_cat = pd.read_csv("apps/dataset/Soles/categoria.csv",parse_dates=['Fecha'])
        ##FILTRAR POR CATEGORIA
        categorias = list(dataset_cat.Categoria.unique())
    
    elif level_data == 'Toneladas':
        # SKU
        dataset_sku = pd.read_csv("apps/dataset/Toneladas/sku.csv",parse_dates=['Fecha'])
        # FILTRAR POR SKU
        skus = list(dataset_sku.SKU.unique())
        # NIVEL MARCA
        dataset_marca = pd.read_csv("apps/dataset/Toneladas/marca.csv",parse_dates=['Fecha'])
        # FILTRAR POR MARCA
        marcas_prod = list(dataset_marca.Marca.unique())
        # NIVEL CATEGORIA
        dataset_cat = pd.read_csv("apps/dataset/Toneladas/categoria.csv",parse_dates=['Fecha'])
        # FILTRAR POR CATEGORIA
        categorias = list(dataset_cat.Categoria.unique())

    level = st.sidebar.radio('Selecciona el nivel:',('SKU','Marca','Categoria'))

    if level == 'SKU':
        dataset = dataset_sku
        prod_select = st.sidebar.multiselect('Selecciona los productos:', skus)
        for i in prod_select:
            process(dataset,diccionario,level_data,level,i,index_col="Fecha")

    elif level == 'Marca':
        dataset = dataset_marca
        marca_select = st.sidebar.multiselect('Selecciona las marcas:',marcas_prod)
        for i in marca_select:
            process(dataset,diccionario,level_data,level,i,index_col="Fecha")

    else:
        dataset = dataset_cat
        cat_select = st.sidebar.multiselect('Selecciona las categorias:', categorias)
        for i in cat_select:
            process(dataset,diccionario,level_data,level,i,index_col="Fecha")