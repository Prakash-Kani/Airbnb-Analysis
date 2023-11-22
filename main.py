import pandas as pd
import numpy as np
import datetime as dt
import plotly.express as px
import mysql.connector

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

mysql_host_name = os.getenv("MYSQL_HOST_NAME")
mysql_user_name = os.getenv("MYSQL_USER_NAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database_name = os.getenv("MYSQL_DATABASE_NAME")

mydb = mysql.connector.connect(host = mysql_host_name,
                             user = mysql_user_name,
                             password = mysql_password,
                             database = mysql_database_name)
mycursor = mydb.cursor(buffered = True)

def Country():
    mycursor.execute('SELECT DISTINCT(Country) FROM vacation_rental_listings;')
    data=mycursor.fetchall()
    country = [country[0] for country in data]
    return country

def Suburb(country):
    if country == 'Select All':
        query = 'SELECT DISTINCT(Suburb) FROM vacation_rental_listings;'
    else:
        query = f'SELECT DISTINCT(Suburb) FROM vacation_rental_listings WHERE Country ="{country}";'
    mycursor.execute(query)
    data=mycursor.fetchall()
    suburb = [suburb[0] for suburb in data]
    return suburb

def Bed_room(country, suburb):
    if country == 'Select All' and suburb == 'Select All':
        query = 'SELECT DISTINCT(bedrooms) FROM vacation_rental_listings;'
    elif country == 'Select All':
        query = f'SELECT DISTINCT(bedrooms) FROM vacation_rental_listings WHERE Suburb ="{suburb}";'
    elif suburb == 'Select All':
        query = f'SELECT DISTINCT(bedrooms) FROM vacation_rental_listings WHERE Country ="{country}";'
    else:
        query = f'SELECT DISTINCT(bedrooms) FROM vacation_rental_listings WHERE Country ="{country}" and Suburb ="{suburb}";'
    mycursor.execute(query)
    data=mycursor.fetchall()
    bed_rooms = [rooms[0] for rooms in data]
    return bed_rooms


def Geo_fig(country, suburb, bedrooms):
    if country == 'Select All':
        countrycondition=''
    else:
        countrycondition = f"Country ='{country}'"
    if suburb == 'Select All':
        suburbcondition=''
    else:
        suburbcondition = f"Suburb ='{suburb}'"
    if bedrooms == 'Select All':
        bedroomscondition=''
    else:
        bedroomscondition = f"bedrooms ='{bedrooms}'"
    

    query= "SELECT Suburb, Country, Price, Latitude, Longitude FROM vacation_rental_listings WHERE Is_Location_Exact = True"
    if countrycondition !='' and suburbcondition != '' and bedroomscondition !='':
        query = query + ' and ' + countrycondition + ' and ' +suburbcondition + ' and ' + bedroomscondition+';'
    
    elif countrycondition !='' and suburbcondition != '':
        query = query + ' and ' + countrycondition + ' and ' +suburbcondition + ';'
    elif suburbcondition != '' and bedroomscondition !='':
        query = query + ' and ' + suburbcondition + ' and ' + bedroomscondition+';'
    elif countrycondition !='' and bedroomscondition !='':
        query = query + ' and ' + countrycondition + ' and ' + bedroomscondition+';'
    elif countrycondition !='':
        query = query + ' and ' + countrycondition + ';'
    elif suburbcondition != '':
        query = query + ' and ' + suburbcondition + ';'
    elif bedroomscondition !='':
        query = query + ' and ' + bedroomscondition+';'
    else:
        pass
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.scatter_mapbox(df,
                            lat=df['Latitude'],
                            lon=df['Longitude'],
                            hover_name="Suburb",
                            zoom=10,
                            mapbox_style="satellite"
                        )  
    fig = px.scatter_geo(df,
                        lat=df.Latitude,
                        lon=df.Longitude,
                        hover_name="Country",
                        size='Price',
                        color='Suburb',
                        color_continuous_scale='Price', height = 400, width = 600
                        )
    fig.update_layout(showlegend=False)
    return fig

def to_query(query, country, suburb, bedrooms):
    if country == 'Select All':
        countrycondition=''
    else:
        countrycondition = f"Country ='{country}'"
    if suburb == 'Select All':
        suburbcondition=''
    else:
        suburbcondition = f"Suburb ='{suburb}'"
    if bedrooms == 'Select All':
        bedroomscondition=''
    else:
        bedroomscondition = f"bedrooms ='{bedrooms}'"
    if countrycondition !='' and suburbcondition != '' and bedroomscondition !='':
        query = query + ' WHERE ' + countrycondition + ' and ' +suburbcondition + ' and ' + bedroomscondition
    
    elif countrycondition !='' and suburbcondition != '':
        query = query + ' WHERE ' + countrycondition + ' and ' +suburbcondition 
    elif suburbcondition != '' and bedroomscondition !='':
        query = query + ' WHERE ' + suburbcondition + ' and ' + bedroomscondition
    elif countrycondition !='' and bedroomscondition !='':
        query = query + ' WHERE ' + countrycondition + ' and ' + bedroomscondition
    elif countrycondition !='':
        query = query + ' WHERE ' + countrycondition
    elif suburbcondition != '':
        query = query + ' WHERE ' + suburbcondition 
    elif bedroomscondition !='':
        query = query + ' WHERE ' + bedroomscondition
    return query

def Bed_price(country, suburb, bedrooms):
    query = """SELECT room_type, bed_type,AVG(price) AS Avg_bed_price FROM vacation_rental_listings"""
    query = to_query(query, country, suburb, bedrooms)
    query = query + " GROUP BY room_type,bed_type"
    # print(query)
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.sunburst(df, path=['room_type', 'bed_type',], values='Avg_bed_price', color = 'bed_type',  height= 500, width = 500)
    return fig

def room_price(country, suburb, bedrooms):
    query = """SELECT room_type,AVG(price) AS Avg_room_price FROM vacation_rental_listings"""
    query = to_query(query, country, suburb, bedrooms)
    query = query + " GROUP BY room_type"
    # print(query)
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.pie(df, values='Avg_room_price', names='room_type', hole=.6,labels='Avg_room_price')
    return fig