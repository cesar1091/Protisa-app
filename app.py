import streamlit as st
from multiapp import MultiApp
from apps import  producto, home, distribuidoras

# import your app modules here

app = MultiApp()

# Add all your application here
app.add_app("Home", home.app)
app.add_app("Distribuidor", distribuidoras.app)
app.add_app("Producto", producto.app)

# The main app
app.run()
