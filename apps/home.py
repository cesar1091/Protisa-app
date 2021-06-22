import streamlit as st
from PIL import Image

def app():
    image = Image.open('apps/BANNER-PROTISA.jpg')
    st.image(image, caption='Project test')
    st.markdown("""
    # Web app
    **Prediccion de ventas sell out protisa diario**\n
    Los modelos desarrollados estan basados en modelos XGBoost \n
    ![Logo](https://algorithmclasses.files.wordpress.com/2020/09/xgboost-logo.png?w=318 "XGBoost")

    """)
