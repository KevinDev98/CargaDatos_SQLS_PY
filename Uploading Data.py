# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 13:04:35 2022
@author: Kevin
"""
import pyodbc
import pandas as pd

df=pd.read_csv('C:\\Users\Kevin\Documents\KEVIN\PROYECTOS DEV\CARGA DATOS\DATA FILES\CARGA_CSV\CIUDAD_CSV.csv')
srvr ='DESKTOP-EB8Q5MG'
db='VENTAS_CIUDAD'
username='sa'
pwd='inginf98'
try:    
    StrConnect='DRIVER={SQL Server};SERVER='+srvr+';DATABASE='+db+';UID='+username+';PWD='+pwd
    #print(StrConnect)
    Connection=pyodbc.connect(StrConnect)
    print('Conexion establecida con SQL SERVER')
except Exception as ex:
    print('Error Connection: {}' .format(ex))

"""
Limpia espacios en blando
"""
def Remove_spacewith(s): #Elimina espacios del final
    try:        
        if s.endswith(" "): s = s.rstrip()
        if s.startswith(" "): s = s.lstrip()
    except Exception as ex:
        s='Error'
        print('Error Cleanning: {}' .format(ex))
    return s

##INICIA CURSOR    
cursor = Connection.cursor()

try:    
     for index, row in df.iterrows(): #Recorre las lineas del csv
         try:        
             # print('inicia proceso')
             id_city=Remove_spacewith(row.VNT_CVE_CIUDAD)
             id_region=Remove_spacewith(row.VNT_CVE_REGION)
             nombreCity=Remove_spacewith(row.VNT_NOMBRE_CIUDAD)             
             id_city=id_city.strip()
             id_region=id_region.strip()
             nombreCity=nombreCity.strip()
             #print("id_city:"+ id_city + "id_region:" + id_region + "nombre ciudad:" + nombreCity)
             cursor.execute("INSERT INTO VNT_CAT_CIUDADES(VNT_CVE_CIUDAD,VNT_CVE_REGION,VNT_NOMBRE_CIUDAD)VALUES(?,?,?)", id_city, id_region, nombreCity)
             print('Registro insertado')
         except Exception as ex:
             print('Error loading data: {}' .format(ex))  
             #cursor.close()        
except Exception as ex:    
    print('Error Method: {}' .format(ex)) 
    #cursor.close()
    
Connection.commit()
cursor.close()
