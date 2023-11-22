import streamlit as st
import main 

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
col1, col2, col3=st.columns([1,1,1], gap = 'large')

with col1:
    country= main.Country()
    country.insert(0,'Select All')
    country =st.selectbox('***Select the Country***', country ,index=0)

with col2:
    suburb = main.Suburb(country)
    suburb.insert(0,'Select All')
    suburb =st.selectbox('***Select the Country***', suburb ,index=0)

with col3:
    bed_rooms = main.Bed_room(country, suburb)
    bed_rooms.insert(0,'Select All')
    bed_rooms =st.selectbox('***Select the Country***', bed_rooms ,index=0)

col4, col5, col6=st.columns([2,2,2], gap = 'small')

with col4:
    fig = main.Geo_fig(country, suburb, bed_rooms)
    st.plotly_chart(fig)
with col5:
    bed_price_fig =main.Bed_price(country, suburb, bed_rooms)

    st.plotly_chart(bed_price_fig)
# st.write(bed_price_fig)
with col6:
    room_price_fig =main.room_price(country, suburb, bed_rooms)

    st.plotly_chart(room_price_fig)
