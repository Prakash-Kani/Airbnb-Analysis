import streamlit as st
import main 
import Extract_Data as ED
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

st.set_page_config(
    page_title="AirBnB Analysis",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.extremelycoolapp.com/help',
        'Report a bug': "https://www.extremelycoolapp.com/bug",
        'About': "# This is a header. This is an *extremely* cool app!"
    }
)

st.header(':red[Airbnb Data Analysis]', divider = 'rainbow')
col1, col2, col3, col_4=st.columns([1,1,1,1], gap = 'large')

with col1:
    extract = st.button('Extract & Migration')
    if extract:
        username =os.getenv("MONGODB_USER_NAME")
        password = os.getenv('MONGODB_PASSWORD')
        airbnb = ED.Extract_Datas(username, password)
        ED.Create_Table()
        ED.Insert(airbnb)

with col2:
    country= main.Country()
    country.insert(0,'Select All')
    country =st.selectbox('***Select the Country***', country ,index=0)

with col3:
    suburb = main.Suburb(country)
    suburb.insert(0,'Select All')
    suburb =st.selectbox('***Select the Suburb***', suburb ,index=0)

with col_4:
    bed_rooms = main.Bed_room(country, suburb)
    bed_rooms.insert(0,'Select All')
    bed_rooms =st.selectbox('***Select the Bed Room Count***', bed_rooms ,index=0)

col4, col5, col6=st.columns([2,2,2], gap = 'small')

with col4:
    fig = main.Geo_fig(country, suburb, bed_rooms)
    st.plotly_chart(fig)

    st.plotly_chart(main.top_10_suburb_properties(country, suburb, bed_rooms))

    st.plotly_chart(main.min_nights_properties(country, suburb, bed_rooms))

    st.plotly_chart(main.max_nights_properties(country, suburb, bed_rooms))
with col5:
    st.plotly_chart(main.bed_price_pie(country, suburb, bed_rooms))
    bed_price_fig =main.Bed_price(country, suburb, bed_rooms)

    st.plotly_chart(bed_price_fig)

    st.plotly_chart(main.property_type_fig(country, suburb, bed_rooms))

    st.plotly_chart(main.host_reviews_fig(country, suburb, bed_rooms))
with col6:
    room_price_fig =main.room_price(country, suburb, bed_rooms)

    st.plotly_chart(room_price_fig)

    st.plotly_chart(main.top_10_host_properties(country, suburb, bed_rooms))

    st.plotly_chart(main.cancellation_fig(country, suburb, bed_rooms))

    st.plotly_chart(main.guests_price_fig(country, suburb, bed_rooms))
