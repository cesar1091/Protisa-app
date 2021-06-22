import pandas as pd
import pyodbc
import numpy as np


sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=51.222.82.146;DATABASE=STRATEGIO_OLAP_PROTISA;UID=Cesar_VS;PWD=@tenasmf626;Trusted_Connection=no')

#Cierre de ventas(soles)

#Distribuidor
query_dist = "WITH TABLA_DIST AS (SELECT d.NombreDistribuidor AS Distribuidor,a.CodigoFecha AS Fecha,ROUND(SUM(a.VentaSinIgv),2) AS Soles FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente =e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY d.NombreDistribuidor,a.CodigoFecha) SELECT Distribuidor,Fecha,Soles FROM TABLA_DIST"
distribuidor_df = pd.read_sql(query_dist,sql_conn)
distribuidor_df['Fecha'] = pd.to_datetime(distribuidor_df['Fecha'],infer_datetime_format=True)
distribuidor_df = distribuidor_df[distribuidor_df.Distribuidor.isin(distribuidor_df.Distribuidor.value_counts()[:80].index)]
distribuidor_df['Soles'] = np.abs(distribuidor_df['Soles'])
distribuidor_df.to_csv("dataset/Soles/distribuidor.csv")
print(distribuidor_df.head(5),'\n')
#Localidad
query_loc = "WITH TABLA AS (SELECT d.Localidad AS Localidad,a.CodigoFecha AS Fecha,ROUND(SUM(a.VentaSinIgv),2) AS Soles FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY d.Localidad,a.CodigoFecha) SELECT Localidad,Fecha,Soles FROM TABLA"
localidad_df = pd.read_sql(query_loc,sql_conn)
localidad_df['Fecha'] = pd.to_datetime(localidad_df['Fecha'],infer_datetime_format=True)
localidad_df = localidad_df.replace('CUSCO','CUZCO')
localidad_df = localidad_df.replace('CAMANA','AREQUIPA')
localidad_df = localidad_df.replace('CHACHAPOYAS','AMAZONAS')
localidad_df = localidad_df.replace('CHINCHA','ICA')
localidad_df = localidad_df.replace('HUACHO','LIMA')
localidad_df = localidad_df.replace('JAEN','CAJAMARCA')
localidad_df = localidad_df.groupby(['Localidad','Fecha']).sum().reset_index()
localidad_df.to_csv("dataset/Soles/localidad.csv")
print(localidad_df.head(5),'\n')

#Region
query_region = "WITH TABLA AS (SELECT d.Region AS Region, a.CodigoFecha AS Fecha, ROUND(SUM(a.VentaSinIgv),2) AS Soles FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL  AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY d.Region,a.CodigoFecha) SELECT Region, Fecha, Soles FROM TABLA"
region_df = pd.read_sql(query_region,sql_conn)
region_df['Fecha'] = pd.to_datetime(region_df['Fecha'],infer_datetime_format=True)
region_df['Soles'] = np.abs(region_df['Soles'])
region_df.to_csv("dataset/Soles/region.csv")
print(region_df.head(5),'\n')

#SKU
query_sku = "WITH TABLA AS (SELECT c.NombreProducto AS SKU,a.CodigoFecha AS Fecha,ROUND(SUM(a.VentaSinIgv),2) AS Soles FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY c.NombreProducto,a.CodigoFecha) SELECT SKU,Fecha,Soles FROM TABLA"
dataset_sku = pd.read_sql(query_sku,sql_conn)
dataset_sku.dropna(inplace=True)
dataset_sku['Fecha'] = pd.to_datetime(dataset_sku['Fecha'],infer_datetime_format=True)
dataset_sku['Soles'] = np.abs(dataset_sku['Soles'])
dataset_sku = dataset_sku[dataset_sku.SKU.isin(dataset_sku.groupby('SKU')['Soles'].agg([np.sum,'count']).sort_values(['count','sum'],ascending=False)[:50].index)]
dataset_sku.to_csv("dataset/Soles/sku.csv")
print(dataset_sku.head(5),'\n')
#Marca
query_marca = "WITH TABLA AS (SELECT c.Marca AS Marca,a.CodigoFecha AS Fecha,ROUND(SUM(a.VentaSinIgv),2) AS Soles FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY c.Marca,a.CodigoFecha) SELECT Marca,Fecha,Soles FROM TABLA"
dataset_marca = pd.read_sql(query_marca,sql_conn)
dataset_marca.dropna(inplace=True)
dataset_marca['Fecha'] = pd.to_datetime(dataset_marca['Fecha'],infer_datetime_format=True)
dataset_marca['Soles'] = np.abs(dataset_marca['Soles'])
dataset_marca.to_csv('dataset/Soles/marca.csv')
print(dataset_marca.head(5),'\n')
#Categoria
query_cat = "WITH TABLA AS (SELECT c.Categoria AS Categoria,a.CodigoFecha AS Fecha,ROUND(SUM(a.VentaSinIgv),2) AS Soles FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY c.Categoria,a.CodigoFecha) SELECT Categoria,Fecha,Soles FROM TABLA"
dataset_cat = pd.read_sql(query_cat,sql_conn)
dataset_cat.dropna(inplace=True)
dataset_cat['Fecha'] = pd.to_datetime(dataset_cat['Fecha'],infer_datetime_format=True)
dataset_cat['Soles'] = np.abs(dataset_cat['Soles'])
dataset_cat.to_csv('dataset/Soles/categoria.csv')
print(dataset_cat.head(5),'\n')

#Cierre de ventas(Cantidad)
# Distribuidoras
query_dist = "WITH TABLA_DIST AS (SELECT d.NombreDistribuidor AS Distribuidor,a.CodigoFecha AS Fecha,ROUND(SUM(a.CantidadUC),2) AS Cantidad FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente =e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY d.NombreDistribuidor,a.CodigoFecha) SELECT Distribuidor,Fecha,Cantidad FROM TABLA_DIST"
distribuidor_df = pd.read_sql(query_dist,sql_conn)
distribuidor_df['Fecha'] = pd.to_datetime(distribuidor_df['Fecha'],infer_datetime_format=True)
distribuidor_df = distribuidor_df[distribuidor_df.Distribuidor.isin(distribuidor_df.Distribuidor.value_counts()[:80].index)]
distribuidor_df.to_csv("dataset/Cantidad/distribuidor.csv")
print(distribuidor_df.head(5),'\n')
#Localidad
query_loc="WITH TABLA AS (SELECT d.Localidad AS Localidad,a.CodigoFecha AS Fecha,ROUND(SUM(a.CantidadUC),2) AS Cantidad FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY d.Localidad,a.CodigoFecha) SELECT Localidad,Fecha,Cantidad FROM TABLA"
localidad_df = pd.read_sql(query_loc,sql_conn)
localidad_df['Fecha'] = pd.to_datetime(localidad_df['Fecha'],infer_datetime_format=True)
localidad_df = localidad_df.replace('CUSCO','CUZCO')
localidad_df = localidad_df.replace('CAMANA','AREQUIPA')
localidad_df = localidad_df.replace('CHACHAPOYAS','AMAZONAS')
localidad_df = localidad_df.replace('CHINCHA','ICA')
localidad_df = localidad_df.replace('HUACHO','LIMA')
localidad_df = localidad_df.replace('JAEN','CAJAMARCA')
localidad_df = localidad_df.groupby(['Localidad','Fecha']).sum().reset_index()
localidad_df.to_csv("dataset/Cantidad/localidad.csv")
print(localidad_df.head(5),'\n')
#Region
query_region = "WITH TABLA AS (SELECT d.Region AS Region,a.CodigoFecha AS Fecha,ROUND(SUM(a.CantidadUC),2) AS Cantidad FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL  AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY d.Region,a.CodigoFecha) SELECT Region, Fecha, Cantidad FROM TABLA"
region_df = pd.read_sql(query_region,sql_conn)
region_df['Fecha'] = pd.to_datetime(region_df['Fecha'],infer_datetime_format=True)
region_df.to_csv("dataset/Cantidad/region.csv")
print(region_df.head(5),'\n')
#SKU
query_sku = "WITH TABLA AS (SELECT c.NombreProducto AS SKU,a.CodigoFecha AS Fecha,ROUND(SUM(a.CantidadUC),2) AS Cantidad FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY c.NombreProducto,a.CodigoFecha) SELECT SKU,Fecha,Cantidad FROM TABLA"
dataset_sku = pd.read_sql(query_sku,sql_conn)
dataset_sku.dropna(inplace=True)
dataset_sku['Fecha'] = pd.to_datetime(dataset_sku['Fecha'],infer_datetime_format=True)
dataset_sku = dataset_sku[dataset_sku.SKU.isin(dataset_sku.groupby('SKU')['Cantidad'].agg([np.sum,'count']).sort_values(['count','sum'],ascending=False)[:50].index)]
dataset_sku.to_csv("dataset/Cantidad/sku.csv")
print(dataset_sku.head(5),'\n')
#Marca
query_marca = "WITH TABLA AS (SELECT c.Marca AS Marca,a.CodigoFecha AS Fecha,ROUND(SUM(a.CantidadUC),2) AS Cantidad FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY c.Marca,a.CodigoFecha) SELECT Marca,Fecha,Cantidad FROM TABLA"
dataset_marca = pd.read_sql(query_marca,sql_conn)
dataset_marca.dropna(inplace=True)
dataset_marca['Fecha'] = pd.to_datetime(dataset_marca['Fecha'],infer_datetime_format=True)
dataset_marca.to_csv('dataset/Cantidad/marca.csv')
print(dataset_marca.head(5),'\n')
#Categoria
query_cat = "WITH TABLA AS (SELECT c.Categoria AS Categoria,a.CodigoFecha AS Fecha,ROUND(SUM(a.CantidadUC),2) AS Cantidad FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Cliente] AS e ON a.CodigoCliente = e.CodigoCliente WHERE a.CodigoFecha>='2019-09-01' AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0 GROUP BY c.Categoria,a.CodigoFecha) SELECT Categoria,Fecha,Cantidad FROM TABLA"
dataset_cat = pd.read_sql(query_cat,sql_conn)
dataset_cat.dropna(inplace=True)
dataset_cat['Fecha'] = pd.to_datetime(dataset_cat['Fecha'],infer_datetime_format=True)
dataset_cat.to_csv('dataset/Cantidad/categoria.csv')
print(dataset_cat.head(5),'\n')

#Cierre de ventas(Toneladas)
#Distribuidoras
query_dist = "WITH TABLA AS (SELECT a.CodigoFecha AS Fecha,d.NombreDistribuidor AS Distribuidor,c.NombreProducto AS Producto,c.Categoria AS Categoria,c.Marca AS Marca,c.Segmento AS Segmento,CASE WHEN c.Segmento = '10 Masivos' THEN c.PESONETOUNI*a.CantidadUC ELSE c.PESONETOUNI*a.CantidadUC*c.PaquetesXBulto END AS Toneladas FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor WHERE a.CodigoFecha>='2019-09-01'AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0) SELECT Distribuidor as Distribuidor,Fecha,SUM(Toneladas) as Toneladas FROM TABLA GROUP BY Distribuidor,Fecha" 
distribuidor_df = pd.read_sql(query_dist,sql_conn)
distribuidor_df['Fecha'] = pd.to_datetime(distribuidor_df['Fecha'],infer_datetime_format=True)
distribuidor_df = distribuidor_df[distribuidor_df.Distribuidor.isin(distribuidor_df.Distribuidor.value_counts()[:80].index)]
distribuidor_df.to_csv("dataset/Toneladas/distribuidor.csv")
print(distribuidor_df.head(5),'\n')
#Localidad
query_loc = "WITH TABLA AS (SELECT a.CodigoFecha AS Fecha,d.NombreDistribuidor AS Distribuidor,c.NombreProducto AS Producto,d.Localidad AS Localidad,d.Region AS Region,c.Categoria AS Categoria,c.Marca AS Marca,c.Segmento AS Segmento,CASE WHEN c.Segmento = '10 Masivos' THEN c.PESONETOUNI*a.CantidadUC ELSE c.PESONETOUNI*a.CantidadUC*c.PaquetesXBulto END AS Toneladas FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor WHERE a.CodigoFecha>='2019-09-01'AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0) SELECT Localidad,Fecha,SUM(Toneladas) as Toneladas FROM TABLA GROUP BY Localidad,Fecha"
localidad_df = pd.read_sql(query_loc,sql_conn)
localidad_df['Fecha'] = pd.to_datetime(localidad_df['Fecha'],infer_datetime_format=True)
localidad_df = localidad_df.replace('CUSCO','CUZCO')
localidad_df = localidad_df.replace('CAMANA','AREQUIPA')
localidad_df = localidad_df.replace('CHACHAPOYAS','AMAZONAS')
localidad_df = localidad_df.replace('CHINCHA','ICA')
localidad_df = localidad_df.replace('HUACHO','LIMA')
localidad_df = localidad_df.replace('JAEN','CAJAMARCA')
localidad_df = localidad_df.groupby(['Localidad','Fecha']).sum().reset_index()
localidad_df.to_csv("dataset/Toneladas/localidad.csv")
print(localidad_df.head(5),'\n')
#Region
query_region = "WITH TABLA AS (SELECT a.CodigoFecha AS Fecha,d.NombreDistribuidor AS Distribuidor,c.NombreProducto AS Producto,d.Localidad AS Localidad,d.Region AS Region,c.Categoria AS Categoria,c.Marca AS Marca,c.Segmento AS Segmento,CASE WHEN c.Segmento = '10 Masivos' THEN c.PESONETOUNI*a.CantidadUC ELSE c.PESONETOUNI*a.CantidadUC*c.PaquetesXBulto END AS Toneladas FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor WHERE a.CodigoFecha>='2019-09-01'AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0) SELECT Region,Fecha,SUM(Toneladas) as Toneladas FROM TABLA GROUP BY Region,Fecha"
region_df = pd.read_sql(query_region,sql_conn)
region_df['Fecha'] = pd.to_datetime(region_df['Fecha'],infer_datetime_format=True)
region_df.to_csv("dataset/Toneladas/region.csv")
print(region_df.head(5),'\n')
#SKU
query_sku = "WITH TABLA AS (SELECT a.CodigoFecha AS Fecha,d.NombreDistribuidor AS Distribuidor,c.NombreProducto AS Producto,d.Localidad AS Localidad,d.Region AS Region,c.Categoria AS Categoria,c.Marca AS Marca,c.Segmento AS Segmento,CASE WHEN c.Segmento = '10 Masivos' THEN c.PESONETOUNI*a.CantidadUC ELSE c.PESONETOUNI*a.CantidadUC*c.PaquetesXBulto END AS Toneladas FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor WHERE a.CodigoFecha>='2019-09-01'AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0) SELECT Producto as SKU,Fecha,SUM(Toneladas) as Toneladas FROM TABLA GROUP BY Producto,Fecha"
dataset_sku = pd.read_sql(query_sku,sql_conn)
dataset_sku.dropna(inplace=True)
dataset_sku['Fecha'] = pd.to_datetime(dataset_sku['Fecha'],infer_datetime_format=True)
dataset_sku = dataset_sku[dataset_sku.SKU.isin(dataset_sku.groupby('SKU')['Toneladas'].agg([np.sum,'count']).sort_values(['count','sum'],ascending=False)[:50].index)]
dataset_sku.to_csv("dataset/Toneladas/sku.csv")
print(dataset_sku.head(5),'\n')
#Marca
query_marca = "WITH TABLA AS (SELECT a.CodigoFecha AS Fecha,d.NombreDistribuidor AS Distribuidor,c.NombreProducto AS Producto,d.Localidad AS Localidad,d.Region AS Region,c.Categoria AS Categoria,c.Marca AS Marca,c.Segmento AS Segmento,CASE WHEN c.Segmento = '10 Masivos' THEN c.PESONETOUNI*a.CantidadUC ELSE c.PESONETOUNI*a.CantidadUC*c.PaquetesXBulto END AS Toneladas FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor WHERE a.CodigoFecha>='2019-09-01'AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0) SELECT Marca,Fecha,SUM(Toneladas) as Toneladas FROM TABLA GROUP BY Marca,Fecha"
dataset_marca = pd.read_sql(query_marca,sql_conn)
dataset_marca.dropna(inplace=True)
dataset_marca['Fecha'] = pd.to_datetime(dataset_marca['Fecha'],infer_datetime_format=True)
dataset_marca.to_csv('dataset/Toneladas/marca.csv')
print(dataset_marca.head(5),'\n') 
#Categoria
query_cat = "WITH TABLA AS (SELECT a.CodigoFecha AS Fecha,d.NombreDistribuidor AS Distribuidor,c.NombreProducto AS Producto,d.Localidad AS Localidad,d.Region AS Region,c.Categoria AS Categoria,c.Marca AS Marca,c.Segmento AS Segmento,CASE WHEN c.Segmento = '10 Masivos' THEN c.PESONETOUNI*a.CantidadUC ELSE c.PESONETOUNI*a.CantidadUC*c.PaquetesXBulto END AS Toneladas FROM [STRATEGIO_OLAP_PROTISA].[pbix].[Ventas] AS a INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Producto] AS c ON a.CodigoProductoDistribuidor = c.CodigoProducto INNER JOIN [STRATEGIO_OLAP_PROTISA].[pbix].[Distribuidor] AS d ON a.CodigoDistribuidor = d.CodigoDistribuidor WHERE a.CodigoFecha>='2019-09-01'AND a.CodigoFecha<=GETDATE() AND a.CodigoDistribuidor not in ('20100239559.0','20100239559.1','20100239559.2','20100239559.3','20100239559.7','20100239559.9') AND c.Marca not in ('Ego','Ideal','Sussy') AND a.CodigoDistribuidor IS NOT NULL AND d.Canal NOT IN ('Farmacia') AND a.VentaSinIgv>0) SELECT Categoria,Fecha,SUM(Toneladas) as Toneladas FROM TABLA GROUP BY Categoria,Fecha"
dataset_cat = pd.read_sql(query_cat,sql_conn)
dataset_cat.dropna(inplace=True)
dataset_cat['Fecha'] = pd.to_datetime(dataset_cat['Fecha'],infer_datetime_format=True)
dataset_cat.to_csv('dataset/Toneladas/categoria.csv')
print(dataset_cat.head(5),'\n')
print('Operacion terminada')