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
 
    fig = px.scatter_geo(df,
                        lat=df.Latitude,
                        lon=df.Longitude,
                        hover_name="Country",
                        size='Price',
                        title = 'Geo - Visualization',
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
    fig = px.pie(df, values='Avg_room_price', names='room_type', hole=.7, labels='Avg_room_price', custom_data= 'room_type')
    fig.update_traces(hovertemplate=None, textposition='outside',
                  textinfo='percent+label', rotation=0)
    fig.add_annotation(dict(x=0.5, y=0.53,  align='center',
                        xref = "paper", yref = "paper",
                        showarrow = False, 
                        text="<span style='font-size: 35px; color=#555; font-family:Times New Roman'>Room Type</span>"))
    return fig

def bed_price_pie(country, suburb, bedrooms):
    query = """SELECT bed_type, AVG(price) AS Avg_room_price FROM vacation_rental_listings"""
    query = to_query(query, country, suburb, bedrooms)
    query = query + " GROUP BY bed_type"
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.pie(df, values='Avg_room_price', names='bed_type', hole=.7, labels='Avg_room_price', custom_data= 'bed_type')
    fig.update_traces(hovertemplate=None, textposition='outside',
                  textinfo='percent+label', rotation=0)
    fig.add_annotation(dict(x=0.5, y=0.53,  align='center',
                        xref = "paper", yref = "paper",
                        showarrow = False, 
                        text="<span style='font-size: 35px; color=#555; font-family:Times New Roman'>Bed Type</span>"))
    return fig

def top_10_suburb_properties(country, suburb, bedrooms):
    query = "SELECT TRIM(Suburb) AS Suburb, COUNT(TRIM(Suburb)) AS Number_of_Properties FROM vacation_rental_listings"
    query = to_query(query, country, suburb, bedrooms)
    if query == "SELECT TRIM(Suburb) AS Suburb, COUNT(TRIM(Suburb)) AS Number_of_Properties FROM vacation_rental_listings":
            query = query +""" Where Suburb IS NOT NULL AND TRIM(Suburb) <> ''
                                GROUP BY TRIM(Suburb)
                                ORDER BY Number_of_Properties DESC LIMIT 10;"""
    else:
        query = query +""" and Suburb IS NOT NULL AND TRIM(Suburb) <> ''
                            GROUP BY TRIM(Suburb)
                            ORDER BY Number_of_Properties DESC LIMIT 10;"""
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.bar(df, x='Suburb', y='Number_of_Properties', text = 'Number_of_Properties', 
                 title='Top 10 Suburb by Number of Properties', color ='Number_of_Properties')
    return fig


def top_10_host_properties(country, suburb, bedrooms):
    query = "SELECT host_name, count(host_name) as Number_of_Properties FROM vacation_rental_listings"
    query = to_query(query, country, suburb, bedrooms)

    query = query +""" GROUP BY host_name ORDER BY Number_of_Properties DESC LIMIT 10;"""
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.bar(df, x='host_name', y='Number_of_Properties', text = 'Number_of_Properties',title='Top 10 Suburb by Number of Properties', color= 'Number_of_Properties')
    return fig

def min_nights_properties(country, suburb, bedrooms):
    query = "SELECT minimum_nights, count(host_name) as Number_of_Properties FROM vacation_rental_listings"
    query = to_query(query, country, suburb, bedrooms)

    query = query +""" GROUP BY minimum_nights ORDER BY Number_of_Properties DESC LIMIT 5;"""
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.bar(df, x='minimum_nights', y='Number_of_Properties', text = 'Number_of_Properties',title='Top 5 Minimun Nights Properties Count', color= 'Number_of_Properties')
    return fig

def max_nights_properties(country, suburb, bedrooms):
    query = "SELECT maximum_nights, count(host_name) as Number_of_Properties FROM vacation_rental_listings"
    query = to_query(query, country, suburb, bedrooms)

    query = query +""" GROUP BY maximum_nights ORDER BY Number_of_Properties DESC LIMIT 5;"""
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    df['maximum_nights'] = df['maximum_nights'].astype(str)
    df['maximum_nights'] = df['maximum_nights'] +'-'
    fig = px.bar(df, x='maximum_nights', y='Number_of_Properties', text = 'Number_of_Properties',title='Top 5 Maximun Nights Properties Count', color= 'Number_of_Properties')
    return fig

def cancellation_fig(country, suburb, bedrooms):
    query = """SELECT cancellation_policy, count(cancellation_policy) as Number_of_Properties FROM vacation_rental_listings"""
    query = to_query(query, country, suburb, bedrooms)
    query = query + " GROUP BY cancellation_policy ORDER BY Number_of_Properties DESC LIMIT 5;"
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    fig = px.pie(df, values='Number_of_Properties', names='cancellation_policy', hole=.7, labels='Number_of_Properties', custom_data= 'cancellation_policy')
    fig.update_traces(hovertemplate=None, textposition='outside',
                  textinfo='percent+label', rotation=0)
    fig.add_annotation(dict(x=0.5, y=0.53,  align='center',
                        xref = "paper", yref = "paper",
                        showarrow = False, 
                        text="<span style='font-size: 25px; color=#555; font-family:Times New Roman'>Cancellation Policy</span>"))
    return fig

def property_type_fig(country, suburb, bedrooms):
    query = "SELECT property_type, count(property_type) as Number_of_Properties FROM vacation_rental_listings"
    query = to_query(query, country, suburb, bedrooms)

    query = query +""" GROUP BY property_type ORDER BY Number_of_Properties DESC LIMIT 10;"""
    mycursor.execute(query)
    data=mycursor.fetchall()
    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])

    fig = px.bar(df, x='property_type', y='Number_of_Properties', text = 'Number_of_Properties',title='Top 10 Property Type', color= 'Number_of_Properties')
    return fig


def host_reviews_fig(country, suburb, bedrooms):
    query = "SELECT host_name,  sum(Number_of_reviews) as Number_of_reviews FROM vacation_rental_listings"
    query = to_query(query, country, suburb, bedrooms)

    query = query +""" GROUP BY host_name ORDER BY Number_of_Reviews DESC LIMIT 10;"""
    mycursor.execute(query)
    data=mycursor.fetchall()

    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])

    fig = px.bar(df, x='host_name', y='Number_of_reviews', text = 'Number_of_reviews',title='Top 10 Host by Review Count',
                  color= 'Number_of_reviews')
    return fig


def guests_price_fig(country, suburb, bedrooms):
    query = """SELECT guests_included, Average_Price_per_Guest
                FROM (SELECT guests_included, ROUND(AVG(extra_people), 1) AS Average_Price_per_Guest
                FROM vacation_rental_listings"""
    query = to_query(query, country, suburb, bedrooms)

    query = query +""" GROUP BY guests_included ORDER BY Average_Price_per_Guest ASC
    LIMIT 10 ) AS subquery ORDER BY Average_Price_per_Guest DESC;"""
    mycursor.execute(query)
    data=mycursor.fetchall()

    df = pd.DataFrame(data, columns = [i[0] for i in mycursor.description])
    
    df['guests_included'] = df['guests_included'].astype(str)
    df['guests_included'] = df['guests_included']+'-'
    fig = px.bar(df, x='guests_included', y='Average_Price_per_Guest', text = 'Average_Price_per_Guest',title='Lowest Price for Guest',
                  color= 'Average_Price_per_Guest')

    return fig