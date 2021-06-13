# -*- coding: utf-8 -*-
"""
Created on Fri Jun 11 22:10:09 2021

@author: Manuel Béjar
@data: Rodrigo Torres (https://github.com/RoTorresT/segunda_vuelta_peru_2021)
"""

import pandas as pd
import numpy as np
import os

os.chdir('C:/Users/Manuel/Documents/CiudadSur/Data/Elecciones/json_try')

files = os.listdir('./segunda_vuelta_peru_2021/')

#%%  CONSOLIDACIÓN ############################################################

df_2v = pd.DataFrame(columns = ['CCODI_UBIGEO', 'TNOMB_LOCAL', 'TDIRE_LOCAL', 'CCENT_COMPU',
                                'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'CCOPIA_ACTA', 'NNUME_HABILM',
                                'OBSERVACION', 'OBSERVACION_TXT', 'N_CANDIDATOS',
                                'TOT_CIUDADANOS_VOTARON', 'PERU_LIBRE', 'FUERZA_POPULAR', 'VALIDOS',
                                'BLANCO', 'NULOS', 'IMPUGNADOS', 'EMITIDOS'])

for i in range(2,len(files)):

    mesa = pd.read_json("./"+files[i])    
    row = pd.DataFrame.from_dict(mesa.iloc[1,0]['presidencial'], orient='index') \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][0]['congresal']],index=['PERU_LIBRE'])) \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][1]['congresal']],index=['FUERZA_POPULAR'])) \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][2]['congresal']],index=['VALIDOS'])) \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][3]['congresal']],index=['BLANCO'])) \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][4]['congresal']],index=['NULOS'])) \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][5]['congresal']],index=['IMPUGNADOS'])) \
        .append(pd.DataFrame([mesa.iloc[1,0]['votos'][6]['congresal']],index=['EMITIDOS'])).transpose()
    df_2v = df_2v.append(row)

id = pd.DataFrame(files[2:len(files)])
id[0] = id[0].str[0:6]
id.columns = ['mesa']
    
df_2v = df_2v.reset_index(drop=True)
df_2v = id.merge(df_2v,left_index=True,right_index=True)


df_2v.to_excel('./pero_2da_vuelta_2021_2.xlsx')
df_2v.to_csv('./pero_2da_vuelta_2021_2.csv')


#%%  PERU, CONTABILIZADAS Y NO ANULADAS #######################################

print('Actas contabilizadas: ',"{:.3%}".format(len(df_2v[df_2v['OBSERVACION_TXT']!='ACTA ELECTORAL PARA ENVÍO AL JEE'])/len(df_2v)))
#Actas contabilizadas al momento de la extracción:  99.616%

# Tomamos las de Perú
df_2v = df_2v[~df_2v['DEPARTAMENTO'].isin(['AFRICA','AMERICA','ASIA','EUROPA','OCEANIA'])]

# Tomamos las contabilizadas normales
df_2v = df_2v[(df_2v['OBSERVACION']=='CONTABILIZADAS NORMALES')]

# Mesas resueltas NULO==EMITIDO (3 ACTAS)
df_2v = df_2v[(df_2v['OBSERVACION_TXT']!='ACTA ELECTORAL RESUELTA') |
              ((df_2v['OBSERVACION_TXT']=='ACTA ELECTORAL RESUELTA') & (df_2v['NULOS']!=df_2v['EMITIDOS']))] \
             [['mesa', 'CCODI_UBIGEO', 'TNOMB_LOCAL', 
               'DEPARTAMENTO', 'PROVINCIA', 'DISTRITO', 'NNUME_HABILM',
               'PERU_LIBRE', 'FUERZA_POPULAR', 'VALIDOS',
               'BLANCO', 'NULOS', 'IMPUGNADOS', 'EMITIDOS']]

# Creamos una única variable de nulo+viciados
df_2v['NULO_VICIADO'] = df_2v['BLANCO'].astype(int)+df_2v['NULOS'].astype(int)

#%%  TAMAÑOS COMPARABLES #############################################

# Mesas por local
df_2v['MESAS_LOCAL'] = df_2v.groupby(['CCODI_UBIGEO','TNOMB_LOCAL'])['mesa'].transform(len)

# Mesas por distrito
df_2v['MESAS_DISTR'] = df_2v.groupby(['CCODI_UBIGEO'])['mesa'].transform(len)

# Mesas por provincia
df_2v['MESAS_PROVI'] = df_2v.groupby(['DEPARTAMENTO','PROVINCIA'])['mesa'].transform(len)


# Locales con 10 o más mesas
df_2v['GRUPO'] = np.where(df_2v['MESAS_LOCAL']>=10,'LOCAL',
                 np.where(df_2v['MESAS_DISTR']>=10,'DISTRITO','PROVINCIA'))

df_2v['GRUPO'] = np.where((df_2v['MESAS_LOCAL']<9) & (df_2v['MESAS_DISTR']>120),
                          'SIN GRUPO',df_2v['GRUPO'])

df_2v['GRUPO'] = np.where((df_2v['GRUPO']=='PROVINCIA') & (df_2v['MESAS_PROVI']>120),
                          'SIN GRUPO',df_2v['GRUPO'])

#%%  FUNCIÓN DEL Z-SCORE #############################################

def modified_zscore(data, col, den, grupo, cons=1.4826):
    """
    Retorna el modified z-score para una variable en porcentaje.
    input:
        data - base de datos
        col  - numerador
        den  - denominador
        grupo- grupo en el cuál se calcula la mediana (local, distrito, provincia)
        cons - corrección de consistencia
    output:
        mod_zscore - modified z-score del porcentaje (col/den), calculado en el grupo de interés
    """    
    percent = data[col].astype(int)/data[den].astype(int)
    
    mediana = data.merge(pd.DataFrame(percent).rename({0:'percent'}, axis=1),
                     left_index=True,right_index=True) \
               .groupby(grupo)['percent'].transform('median')
    
    desvio = np.abs(percent-mediana)
    
    mad = data.merge(pd.DataFrame(desvio).rename({0:'desvio'}, axis=1),
                     left_index=True,right_index=True) \
               .groupby(grupo)['desvio'].transform('median')
    mod_zscore = desvio/(cons*mad)
    
    return mod_zscore, percent, mediana

#%%  Calculamos modified z-score #############################################

# Grupo Local **************
# 1. Votos atípicos en favor de Perú Libre
df_2v['MZSC_PL_l'] = modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[0]

# 2. Votos atípicos en favor de Fuerza Popular
df_2v['MZSC_FP_l'] = modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[0]

# 3. Votos atípicos en nulos/viciados
df_2v['MZSC_NV_l'] = modified_zscore(df_2v,'NULO_VICIADO', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[0]

# 4. Votos atípicos en favor de Fuerza Popular
df_2v['MZSC_AS_l'] = modified_zscore(df_2v,'EMITIDOS', 'NNUME_HABILM',['CCODI_UBIGEO','TNOMB_LOCAL'])[0]

local = [col for col in df_2v if col.endswith('_l')]

for x in local:
    df_2v[x] = np.where(df_2v['GRUPO']=='LOCAL',df_2v[x],np.nan)

# Grupo Distrito **************
# 1. Votos atípicos en favor de Perú Libre
df_2v['MZSC_PL_d'] = modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['CCODI_UBIGEO'])[0]

# 2. Votos atípicos en favor de Fuerza Popular
df_2v['MZSC_FP_d'] = modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['CCODI_UBIGEO'])[0]

# 3. Votos atípicos en nulos/viciados
df_2v['MZSC_NV_d'] = modified_zscore(df_2v,'NULO_VICIADO', 'EMITIDOS',['CCODI_UBIGEO'])[0]

# 4. Votos atípicos en favor de Fuerza Popular
df_2v['MZSC_AS_d'] = modified_zscore(df_2v,'EMITIDOS', 'NNUME_HABILM',['CCODI_UBIGEO'])[0]

distr = [col for col in df_2v if col.endswith('_d')]

for x in distr:
    df_2v[x] = np.where(df_2v['GRUPO']=='DISTRITO',df_2v[x],np.nan)

# Grupo Provincia **************
# 1. Votos atípicos en favor de Perú Libre
df_2v['MZSC_PL_p'] = modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['DEPARTAMENTO','PROVINCIA'])[0]

# 2. Votos atípicos en favor de Fuerza Popular
df_2v['MZSC_FP_p'] = modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['DEPARTAMENTO','PROVINCIA'])[0]

# 3. Votos atípicos en nulos/viciados
df_2v['MZSC_NV_p'] = modified_zscore(df_2v,'NULO_VICIADO', 'EMITIDOS',['DEPARTAMENTO','PROVINCIA'])[0]

# 4. Votos atípicos en favor de Fuerza Popular
df_2v['MZSC_AS_p'] = modified_zscore(df_2v,'EMITIDOS', 'NNUME_HABILM',['DEPARTAMENTO','PROVINCIA'])[0]

provi = [col for col in df_2v if col.endswith('_p')]

for x in provi:
    df_2v[x] = np.where(df_2v['GRUPO']=='PROVINCIA',df_2v[x],np.nan)
    

#%%  Generar alertas ###################################################

# Indicadores únicos que pasan umbral de 3.5
zscore = [col for col in df_2v if col.startswith('MZSC_')]
alarma = [sub.replace('MZSC', 'OUTL') for sub in zscore]

for i in range(0,len(alarma)):
    df_2v[alarma[i]] = np.where(df_2v[zscore[i]].isnull(),np.nan,
                                np.where(df_2v[zscore[i]]>3.5,1,0))

# Para sentido de alarmas tomamos un partido de referencia y calculamos su mediana correspondiente a su grupo 
df_2v['FP_EMIT'] = modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[1]

df_2v['FP_MEDI'] = np.where(df_2v['GRUPO']=='LOCAL',modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[2],
                   np.where(df_2v['GRUPO']=='DISTRITO',modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['CCODI_UBIGEO'])[2],
                   np.where(df_2v['GRUPO']=='PROVINCIA',modified_zscore(df_2v,'FUERZA_POPULAR', 'EMITIDOS',['DEPARTAMENTO','PROVINCIA'])[2],np.nan)))          

df_2v['PL_EMIT'] = modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[1]

df_2v['PL_MEDI'] = np.where(df_2v['GRUPO']=='LOCAL',modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['CCODI_UBIGEO','TNOMB_LOCAL'])[2],
                   np.where(df_2v['GRUPO']=='DISTRITO',modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['CCODI_UBIGEO'])[2],
                   np.where(df_2v['GRUPO']=='PROVINCIA',modified_zscore(df_2v,'PERU_LIBRE', 'EMITIDOS',['DEPARTAMENTO','PROVINCIA'])[2],np.nan)))          

# Indicador único de irregularidad
unica = [col for col in df_2v if col.startswith('OUTL_PL')]+[col for col in df_2v if col.startswith('OUTL_FP')]

df_2v['IRREGULAR'] =  0
for i in range(0,len(unica)):
    df_2v['IRREGULAR'] = np.where(df_2v[unica[i]]==1,1,df_2v['IRREGULAR'])

# Cuántas es la diferencia
df_2v['DIF_MEDI_FP'] = np.where(df_2v['IRREGULAR']==1,df_2v['FP_EMIT']-df_2v['FP_MEDI'],np.nan)
df_2v['DIF_MEDI_PL'] = np.where(df_2v['IRREGULAR']==1,df_2v['PL_EMIT']-df_2v['PL_MEDI'],np.nan)
df_2v['FAVORECE'] = np.where(df_2v['DIF_MEDI_FP'].isnull(), '',
                    np.where(df_2v['DIF_MEDI_FP']<df_2v['DIF_MEDI_PL'],'PL','FP'))

#df_2v    =   df_2v.drop(df_2v.loc[:,'OUTL_PL_l':'OUTL_AS_p'].columns,axis=1)
#df_2v    =   df_2v.drop(['MZSC_NV_l','MZSC_AS_l','MZSC_NV_d','MZSC_AS_d','MZSC_NV_p','MZSC_AS_p'],axis=1)

df_2v['mesa']          = df_2v['mesa'].astype(int).astype(str).str.zfill(6)
df_2v['CCODI_UBIGEO']  = df_2v['CCODI_UBIGEO'].astype(int).astype(str).str.zfill(6)

df_2v.to_excel('./libro_trabajo.xlsx', index=False)
