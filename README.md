# proyecto_mod2
Proyecto python modulo 2

# -*- coding: utf-8 -*-
"""
Created on Sat Jul 13 11:49:54 2024

@author: Rolando Ticona
"""

import pandas as pd 
import numpy as np

from sqlalchemy import create_engine

#%%Carga de Datos

DATABASE_URL = "postgresql+psycopg2://avnadmin:AVNS_XLT3JzTyx96m_X2pH5w@pg-diplomado-utb-diplomado-utb-2024.c.aivencloud.com:24354/economic_kpis_utb"

engine = create_engine(DATABASE_URL)

# Se extrae toda la información correspondiente a las ciudades asignadas
sql_query = "select yv.* from yearly_value yv inner join country_info ci on yv.country_info_id = ci.id where num_ci = 4738673 "
dfg = pd.read_sql(sql_query, con=engine)

#%%Calcular indicador de ciudad por año

# Se selecciona las columnas necesarias
dfg1 = dfg[['country_info_id', 'indicator_id','year','value']]

# Se extrae la lista de ciudades asignadas
sql_query = "select id country_id, country_name from country_info where num_ci = 4738673"
df_country = pd.read_sql(sql_query, con=engine)

# Se extrae la lista de indicadores necesarios para el calculo del IPS
sql_query = "select id indi_id, indicator_code indi_cod from indicator where id > 1"
df_indicator = pd.read_sql(sql_query, con=engine)

# Se arma un dataframe entre todas las ciudades asignadas y todos los indicadores
df_merge1 = pd.merge(df_country[['country_id']],df_indicator[['indi_id']], how='cross')

# Se crea un dataframe con todos los años
df_anio = pd.DataFrame({'anio': [2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]})

# Se arma un dataframe entre todas las ciudades, los indicadores y todos los años
df_merge2 = df_merge1.merge(df_anio, how='cross')

dfg1 = dfg1.rename(columns={"country_info_id":"country_id"})
dfg1 = dfg1.rename(columns={"indicator_id":"indi_id"})
dfg1 = dfg1.rename(columns={"year":"anio"})

# El dataframe construido se relaciona con el dataframe donde estan los indicadores extraidos
dfg2 =pd.merge(df_merge2, dfg1, on=['country_id','indi_id','anio'],how='left')
dfg2 = dfg2.reset_index()

#todos los vacios rellenar con el dato vacio
#dfg2 = dfg2.fillna(0)

# Como existen años y indicadores nulos, se llena los valores faltantes con el promedio del indicador

dfg2['value'] = dfg2.groupby(['indi_id'])['value'].transform(lambda x:x.fillna(x.median()))

# Se crea un pivot para poder realizar operaciones por filas

pivot_df = dfg2.pivot(index=['country_id','anio'], columns='indi_id', values = 'value')
pivot_df = pivot_df.rename(columns={34:"Y"})
pivot_df = pivot_df.rename(columns={35:"G"})
pivot_df = pivot_df.rename(columns={36:"pov"})
pivot_df = pivot_df.rename(columns={37:"P"})
pivot_df = pivot_df.rename(columns={38:"IPS"})


# Calcular el IPS
pivot_df["IPS"] = (pivot_df["Y"]/pivot_df["P"])*(1-pivot_df["G"])*(1-pivot_df["pov"])

df_ind_calc = pivot_df.reset_index()


for i in df_ind_calc.index:
    print(df_ind_calc.at[i,'country_id'],df_ind_calc.at[i,'anio'],df_ind_calc.at[i,'IPS'])
    


#%% definicion de clases y estructuras

from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

# Create a base class for the declarative model
Base = declarative_base()

# Define the country_info table
class CountryInfo(Base):
    __tablename__ = 'country_info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String, nullable=False)
    country_code = Column(String, unique=True, nullable=False)
    region = Column(String, nullable=False)
    income_group = Column(String, nullable=False)
    num_ci = Column(Integer, nullable=False)
    
    yearly_values = relationship("YearlyValue", back_populates="country_info")

# Define the indicator table
class Indicator(Base):
    __tablename__ = 'indicator'
    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_name = Column(String, nullable=False)
    indicator_code = Column(String, unique=True, nullable=False)
    topic = Column(String, nullable=False)
    
    yearly_values = relationship("YearlyValue", back_populates="indicator")

# Define the yearly_values table
class YearlyValue(Base):
    __tablename__ = 'yearly_value'
    id = Column(Integer, primary_key=True, autoincrement=True)
    year = Column(Integer, nullable=False)
    value = Column(Float, nullable=False)
    country_info_id = Column(Integer, ForeignKey('country_info.id'), nullable=False)
    indicator_id = Column(Integer, ForeignKey('indicator.id'), nullable=False)

    country_info = relationship("CountryInfo", back_populates="yearly_values")
    indicator = relationship("Indicator", back_populates="yearly_values")
    
 # Create a session
Session = sessionmaker(bind=engine)
session = Session()

#%% insertar el indicador en la base de datos

df_ind_calc.dtypes


#df_ind_calc['anio'].dtype

for i in df_ind_calc.index:
    new_yearly_value = YearlyValue(
     year=int(df_ind_calc.at[i,'anio']),
     value=float(df_ind_calc.at[i,'IPS']),
     country_info_id=int(df_ind_calc.at[i,'country_id']),
     indicator_id=38
    )
    session.add(new_yearly_value)
    session.commit()
    
session.rollback()
