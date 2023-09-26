# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import functions as F
from snowflake.snowpark import types as T
import plotly.graph_objects as go
import pandas as pd
import altair as alt
import numpy as np
import pydeck as pdk
import json

st.set_page_config(layout="wide")
# Write directly to the app
st.title('VEHICLE INCIDENTS IN SCOTLAND')
st.subheader('Vehicle Incidents occuring within a health board')

# Get the current credentials
session = get_active_session()

# load raw dataframes
boards = session.table('HEALTH_BOARDS')
board_names = boards.select('HEALTH_BOARD_NAME').distinct().to_pandas()

#select the health board to filter all the data

bname = st.selectbox('Choose Health Board: ', board_names)

#filter the healthboard boundaries to only have the chosen healthboard - also locate the centroid
#so we can position the map

board_filtered2 = boards.filter(F.col('HEALTH_BOARD_NAME') == bname)\
.with_column('centroid',F.call_function('ST_CENTROID',F.col('GEOGRAPHY')))\
.with_column('LATITUDE',F.call_function('ST_Y',F.col('CENTROID')))\
.with_column('LONGITUDE',F.call_function('ST_X',F.col('CENTROID')))


#load the board details to pandas so we can print out the co-ordinates
boardinfo = board_filtered2.to_pandas()


#position the coordinates into 2 columns
geoma,geomb = st.columns(2)



with geoma:
    st.write(f'''Latitude: {boardinfo.LATITUDE.iloc[0]}''')
with geomb:
    st.write(f'''Longitude: {boardinfo.LONGITUDE.iloc[0]}''')


#remove the centroid longitude and latitude to avoid confusion with the co-ordinates 
#of the more detailed datasets

board_filtered = board_filtered2.drop('LONGITUDE','LATITUDE')

IMD = session.table('IMD_2020')

IMD2 = IMD.join(board_filtered,F.call_function('ST_DWITHIN',
                                               IMD['GEOGRAPHY'],
                                               board_filtered['GEOGRAPHY'],20),
                lsuffix='L')


IMD3 = IMD2.select('"DZName"',
                   'GEOGRAPHYL',
                   '"EduAttain"',
                   '"CrimeCount"',
                   '"EmpNumDep"',
                  '"GAccDTGP"')


ACCIDENTS = session.table('UK_VEHICLE_ACCIDENTS')
CASUALTIES = session.table('UK_VEHICLE_CASUALTIES')

weather = session.table('MET_OFFICE_WEATHER_IN_2021')



filtered_weather = weather.join(board_filtered,
                                F.call_function('ST_WITHIN',
                                                weather['POINT'],
                                                board_filtered['GEOGRAPHY']),lsuffix='l').drop('GEOGRAPHY','HEALTH_BOARD_CODE','HEALTH_BOARD_NAME','AREA')

ACCIDENTS_FILTERED = ACCIDENTS.join(board_filtered,
                                    F.call_function('ST_WITHIN',
                                                    ACCIDENTS['POINT']            
                                                    ,board_filtered['GEOGRAPHY']))




ACCIDENTS_IMD = ACCIDENTS_FILTERED.join(IMD3,F.call_function('ST_WITHIN',
                                             ACCIDENTS_FILTERED['POINT'],
                                                  IMD3['GEOGRAPHYL']))\
.group_by(F.col('"DZName"')).agg(F.sum('NUMBER_OF_CASUALTIES').alias('"Casualties Involved in Incidents"')
                                        ,F.mean('"EduAttain"').alias('"Education of school leavers"')
                                        ,F.mean('"CrimeCount"').alias('"Number of Recorded Crimes"')
                                        ,F.mean('"EmpNumDep"').alias('"Number who are Employment Deprived"')
                                        ,F.mean('"GAccDTGP"').alias('"Drivetime to Surgery"')
                                
                                
                                )



map_data2 = ACCIDENTS_FILTERED.drop('POINT','GEOGRAPHY','CENTROID','AREA').to_pandas()


map_data3 = pd.DataFrame()
map_data3['lat'] = map_data2['LATITUDE']
map_data3['lon'] = map_data2['LONGITUDE']




st.pydeck_chart(pdk.Deck(
    map_style=None,
    initial_view_state=pdk.ViewState(
        latitude=boardinfo.LATITUDE.iloc[0],
        longitude=boardinfo.LONGITUDE.iloc[0],
        zoom=8,
        pitch=50,
    ),
    layers=[
        pdk.Layer(
           'HexagonLayer',
           data=map_data3,
           get_position='[lon, lat]',
           radius=400,
           elevation_scale=4,
           elevation_range=[0, 3000],
           pickable=True,
           extruded=True,
        ),
        pdk.Layer(
            'ScatterplotLayer',
            data=map_data3,
            get_position='[lon, lat]',
            get_fill_color=[180, 0, 200, 140],
            get_radius=500,
        ),
    ],
))



def deprivation_columns():
    columns = ACCIDENTS_IMD.drop('"DZName"').columns
    refactored = [s.strip('"') for s in columns]
    return refactored



dep1,dep2,dep3 = st.columns(3)
with dep1:
    x = st.selectbox("Select X Axis:",deprivation_columns())

with dep2:
    y = st.selectbox("Select Y Axis:",deprivation_columns())

with dep3:
    z = st.selectbox("Select Size",deprivation_columns())


st.markdown('#### SCATTER PLOT ANALYSING PATTERNS BETWEEN 3 METRICS')


scatter = alt.Chart(ACCIDENTS_IMD.to_pandas()).mark_point(size=60, opacity=1).encode(
    x=x,
    y=y,
    size=z,
    color=alt.value('pink'),
    tooltip=['DZName', x, y, z]
).interactive()


st.altair_chart(scatter,use_container_width=True)



VEHICLE_ACCIDENTS_DATE = ACCIDENTS_FILTERED.with_column('MONTH',F.monthname('DATE'))\
.with_column('MONTH_NUMBER',F.month('DATE'))\
.group_by(F.col('Month')).agg(F.sum('NUMBER_OF_CASUALTIES').alias('NUMBER_OF_CASUALTIES'),F.sum('NUMBER_OF_VEHICLES').alias('NUMBER_OF_VEHICLES'))

WEATHER_DATE = filtered_weather.with_column('MONTH',F.monthname('DATE')).with_column('MONTH_NUMBER',F.month('DATE'))\
.group_by(F.col('Month')).agg(F.any_value('MONTH_NUMBER').alias('MONTH_NUMBER'),
F.sum('"Total Rainfall Amount in the period 06-21Z"').alias('RAINFALL'),
F.sum('"Max (10min mean) 10m Wind Speed in the period 06-21Z"').alias('WIND'),
F.sum('"Total Snowfall Amount in the period 06-21Z (falling, not necessarily settling)"').alias('SNOW'))

WEATHER_WITH_INCIDENTS = WEATHER_DATE.join(VEHICLE_ACCIDENTS_DATE,'MONTH')

PDW = WEATHER_WITH_INCIDENTS.to_pandas()


data = PDW.reset_index()
base = alt.Chart(data).encode(x='MONTH')


b = base.mark_bar(color='pink').encode(y='NUMBER_OF_CASUALTIES')
a = base.mark_line(color='black').encode(y='SNOW')
d = base.mark_line(color='black').encode(y='RAINFALL')
e = base.mark_line(color='black').encode(y='WIND')

st.markdown('#### VEHICLE INCIDENTS WITH WEATHER DATA')
col1,col2,col3 = st.columns(3)

with col1:
    z = alt.layer(b,a).resolve_scale(y='independent')
    st.altair_chart(z,use_container_width=True)

with col2:
    x = alt.layer(b,d).resolve_scale(y='independent')
    st.altair_chart(x,use_container_width=True)

with col3:
    y = alt.layer(b,e).resolve_scale(y='independent')
    st.altair_chart(y,use_container_width=True)
