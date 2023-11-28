from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime as dt
import mysql.connector

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

username =os.getenv("MONGODB_USER_NAME")
password = os.getenv('MONGODB_PASSWORD')

mysql_host_name = os.getenv("MYSQL_HOST_NAME")
mysql_user_name = os.getenv("MYSQL_USER_NAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database_name = os.getenv("MYSQL_DATABASE_NAME")

mydb = mysql.connector.connect(host = mysql_host_name,
                             user = mysql_user_name,
                             password = mysql_password,
                             database = mysql_database_name)
mycursor = mydb.cursor(buffered = True)

class DataExtraction():
    def __init__(self, username, password):
        cloud_client = MongoClient(f'mongodb+srv://{username}:{password}@cluster0.2owrgpr.mongodb.net/?retryWrites=true')
        database = cloud_client['sample_airbnb']
        self.collection = database['listingsAndReviews'] 

    def  Initial_Data_Extraction(self):
        data = []
        query ={}
        projection = {'_id':True, 'listing_url':True, 'name':True, 'space':True, 'neighborhood_overview':True, 'notes':True, 'transit':True, 
                    'access':True, 'interaction': True, 'house_rules':True, 'property_type':True, 'room_type':True, 'bed_type':True,
                    'minimum_nights':True, 'maximum_nights':True, 'cancellation_policy':True, 'last_scraped':True, 'calendar_last_scraped':True,
                    'first_review':True, 'last_review':True, 'accommodates':True, 'bedrooms':True, 'beds':True, 'number_of_reviews':True, 'bathrooms':True,
                    'amenities':True, 'price':True, 'extra_people':True, 'guests_included':True, 'images.picture_url':True,}

        for i in self.collection.find(query, projection):
            i['images']=i['images']['picture_url']
            i['amenities'] = ', '.join(i['amenities'])

            data.append(i)

        df=pd.DataFrame(data)


        # missing data
        df['beds'].fillna(0, axis=0, inplace = True)
        df['bedrooms'].fillna(0, axis=0, inplace = True)
        df['bathrooms'].fillna(0, axis=0, inplace = True)

        # integer
        df['_id'] = df['_id'].astype(int)
        df['minimum_nights'] = df['minimum_nights'].astype(int)
        df['maximum_nights'] = df['maximum_nights'].astype(int)
        df['accommodates'] = df['accommodates'].astype(int)
        df['bedrooms'] = df['bedrooms'].astype(int)
        df['beds'] = df['beds'].astype(int)
        df['number_of_reviews'] = df['number_of_reviews'].astype(int)

        #float
        df['bathrooms'] = df['bathrooms'].astype(str).astype(float)
        df['price'] = df['price'].astype(str).astype(float)
        df['extra_people'] = df['extra_people'].astype(str).astype(float)
        df['guests_included'] = df['guests_included'].astype(str).astype(float)

        # datetime
        df['last_scraped'] = pd.to_datetime(df['last_scraped'], format = '%Y-%m-%d').dt.date
        df['calendar_last_scraped'] = pd.to_datetime(df['calendar_last_scraped'], format = '%Y-%m-%d').dt.date
        df['first_review'] = pd.to_datetime(df['first_review'], format = '%Y-%m-%d').dt.date
        df['last_review'] = pd.to_datetime(df['last_review'], format = '%Y-%m-%d').dt.date

        self.df = df

        return self.df
    
    def Host_Data(self):
        host=[]
        query ={}
        projection = {'host':True,'_id':True}
        for i in self.collection.find(query, projection):
            i['host']['_id'] = i['_id']
            i['host']["host_verifications"] = ', '.join(i['host']["host_verifications"])

            host.append(i['host'])
            
        hostdf=pd.DataFrame(host)

        hostdf['_id'] = hostdf['_id'].astype(int)
        hostdf['host_id'] = hostdf['host_id'].astype(int) 

        self.hostdf = hostdf
        return self.hostdf 

    def Address_Data(self):
        address_list=[]
        query ={}
        projection = {'address':True,'_id':True}
        for address in self.collection.find(query, projection):

            data = dict(_id = address['_id'],
                        Steet = address['address']['street'],
                        Suburb = address['address']['suburb'],
                        Government_Area = address['address']['government_area'],
                        Market =  address['address']['market'],
                        Country =  address['address']['country'],
                        Country_Code =  address['address']['country_code'],
                        Location_type = address['address']['location']['type'],
                        Longitude = address['address']['location']['coordinates'][0],
                        Latitude = address['address']['location']['coordinates'][1],
                        Is_Location_Exact = address['address']['location']['is_location_exact'],
                        )
            address_list.append(data)

        addressdf = pd.DataFrame(address_list)

        addressdf['_id'] = addressdf['_id'].astype(int)

        self.addressdf = addressdf

        return self.addressdf
    
    def Reviewscore_Data(self):
        reviewscore=[]
        query ={}
        projection = {"review_scores":True, '_id':True}
        for review in self.collection.find(query, projection):
            review['review_scores']['_id'] = review['_id']

            reviewscore.append(review['review_scores'])

        reviewscoredf= pd.DataFrame(reviewscore)

        reviewscoredf['_id'] = reviewscoredf['_id'].astype(int)
        
        self.reviewscoredf =reviewscoredf

        return self.reviewscoredf

    def Availabiliity_Data(self):
        availability_list=[]
        query ={}
        projection = {'availability':True, '_id':True}
        for availability in self.collection.find(query, projection):
            availability['availability']['_id'] = availability['_id']

            availability_list.append(availability['availability'])

        availabilitydf = pd.DataFrame(availability_list)

        availabilitydf['_id'] = availabilitydf['_id'].astype(int)
        
        self.availabilitydf = availabilitydf
        
        return self.availabilitydf
    
    def Review_Data(self):

        review=[]

        for i in self.collection.find({},{'reviews':True,'_id':True}):
            for j in i['reviews']:

                review.append(j)

        reviewdf= pd.DataFrame(review)

        reviewdf['_id'] = reviewdf['_id'].astype(int)
        reviewdf['date'] = pd.to_datetime(reviewdf['date'])
        reviewdf['listing_id'] = reviewdf['listing_id'].astype(int)
        reviewdf['reviewer_id'] = reviewdf['reviewer_id'].astype(int)
        
        self.reviewdf = reviewdf

        return self.reviewdf

    def Merge_Data(self):
        
        self.airbnb = pd.merge(self.df, self.hostdf, on = '_id')
        self.airbnb = pd.merge(self.airbnb, self.addressdf, on = '_id')
        self.airbnb = pd.merge(self.airbnb, self.reviewscoredf, on = '_id')
        self.airbnb = pd.merge(self.airbnb, self.availabilitydf, on = '_id')
        
        return self.airbnb



def Preprocessing(airbnb):

    duplicates = airbnb.duplicated(subset = '_id', keep = 'first')
    if not duplicates.empty:
        airbnb.drop(airbnb[duplicates].index, inplace = True)

    inconsistent_days=airbnb[(airbnb['availability_30'] < 0 ) & (airbnb['availability_30'] >30)]
    if not inconsistent_days.empty:
        airbnb.drop(inconsistent_days.index, inplace = True)

    inconsistent_days=airbnb[(airbnb['availability_60'] <0 ) & (airbnb['availability_60'] >60)]
    if not inconsistent_days.empty:
        airbnb.drop(inconsistent_days.index, inplace = True)

    inconsistent_days=airbnb[(airbnb['availability_90'] <0 ) & (airbnb['availability_90'] >90)]
    if not inconsistent_days.empty:
        airbnb.drop(inconsistent_days.index, inplace = True)

    inconsistent_days=airbnb[(airbnb['availability_365'] <0 ) & (airbnb['availability_365'] >365)]
    if not inconsistent_days.empty:
        airbnb.drop(inconsistent_days.index, inplace = True)

    inconsistent_dates = airbnb[(airbnb['last_review'] > dt.date.today()) | (airbnb['first_review']> dt.date.today())]
    if not inconsistent_dates.empty:
        airbnb.drop(inconsistent_dates.index, inplace = True)

    inconsistent_dates = airbnb[airbnb['first_review'] > airbnb['last_review']]
    if not inconsistent_dates.empty:
        airbnb.drop(inconsistent_dates.index, inplace = True)

    is_host_response = np.where((airbnb['host_response_time'].isna() == True) & (airbnb['host_response_rate'].isna() == True), 0, 1)
    if len(is_host_response):
        airbnb['is_host_response'] = is_host_response

        airbnb = airbnb.fillna({'host_response_time':0,
                                'host_response_rate':0})
        
    is_review_scores = np.where((airbnb['review_scores_accuracy'].isna() == True) & (airbnb['review_scores_cleanliness'].isna() == True) &
                                (airbnb['review_scores_checkin'].isna() == True) & (airbnb['review_scores_communication'].isna() == True) &
                                (airbnb['review_scores_location'].isna() == True) & (airbnb['review_scores_value'].isna() == True) &
                                (airbnb['review_scores_rating'].isna() == True)
                                , 0, 1)
    
    if len(is_review_scores):
        airbnb['is_review_scores'] = is_review_scores

        airbnb = airbnb.fillna({'review_scores_accuracy':0,
                                'review_scores_cleanliness':0,
                                'review_scores_checkin':0,
                                'review_scores_communication':0,
                                'review_scores_location':0,
                                'review_scores_value':0,
                                'review_scores_rating':0,})

    return airbnb



def Extract_Datas(username, password): 
    
    data = DataExtraction(username, password)
    data.Initial_Data_Extraction()
    data.Host_Data()
    data.Address_Data()
    data.Reviewscore_Data()
    data.Availabiliity_Data()

    airbnb = data.Merge_Data()

    airbnb=Preprocessing(airbnb)

    return airbnb

def Create_Table():
    mycursor.execute("""CREATE TABLE IF NOT EXISTS vacation_rental_listings(
                        _id INT PRIMARY KEY,
                        listing_url VARCHAR(250),
                        name TEXT,
                        space TEXT,
                        neighborhood_overview LONGTEXT,
                        notes LONGTEXT,
                        transit LONGTEXT,
                        access LONGTEXT,
                        interaction LONGTEXT,
                        house_rules LONGTEXT,
                        property_type VARCHAR(250),
                        room_type VARCHAR(250),
                        bed_type VARCHAR(250),
                        minimum_nights INT,
                        maximum_nights INT,
                        cancellation_policy VARCHAR(250),
                        last_scraped DATE,
                        calendar_last_scraped DATE,
                        first_review DATE,
                        last_review DATE,
                        accommodates INT,
                        bedrooms INT,
                        beds INT, 
                        number_of_reviews INT, 
                        bathrooms INT, 
                        amenities TEXT,
                        price FLOAT,
                        extra_people FLOAT,
                        guests_included FLOAT,
                        images VARCHAR(250),
                        host_id INT,
                        host_url VARCHAR(250),
                        host_name VARCHAR(250),
                        host_location VARCHAR(250),
                        host_about LONGTEXT,
                        host_thumbnail_url VARCHAR(250),
                        host_picture_url VARCHAR(250),
                        host_neighbourhood VARCHAR(250),
                        host_is_superhost BOOL,
                        host_has_profile_pic BOOL,
                        host_identity_verified BOOL,
                        host_listings_count INT,
                        host_total_listings_count INT,
                        host_verifications VARCHAR(250),
                        host_response_time VARCHAR(250),
                        host_response_rate FLOAT,
                        Street VARCHAR(250),
                        Suburb VARCHAR(250),
                        Government_Area VARCHAR(250),
                        Market VARCHAR(250),
                        Country VARCHAR(250),
                        Country_Code VARCHAR(250),
                        Location_type VARCHAR(250),
                        Longitude FLOAT,
                        Latitude FLOAT,
                        Is_Location_Exact BOOL,
                        review_scores_accuracy FLOAT,
                        review_scores_cleanliness FLOAT,
                        review_scores_checkin FLOAT,
                        review_scores_communication FLOAT,
                        review_scores_location FLOAT,
                        review_scores_value FLOAT,
                        review_scores_rating FLOAT,
                        availability_30 INT,
                        availability_60 INT,
                        availability_90 INT,
                        availability_365 INT,
                        is_host_response BOOL,
                        is_review_scores BOOL);
                        """)
    mydb.commit()
    return 'Sucessfully Table Created!'


def Insert(airbnb):
    query =f"""INSERT INTO vacation_rental_listings
            (_id, listing_url, name, space, neighborhood_overview, notes, transit, access, interaction, house_rules, property_type, room_type, bed_type, minimum_nights, maximum_nights,
            cancellation_policy, last_scraped, calendar_last_scraped, first_review, last_review, accommodates, bedrooms, beds, number_of_reviews, bathrooms, amenities, 
            price, extra_people, guests_included, images, host_id, host_url, host_name, host_location, host_about, host_thumbnail_url,
            host_picture_url, host_neighbourhood,  host_is_superhost, host_has_profile_pic, host_identity_verified, host_listings_count,
            host_total_listings_count, host_verifications, host_response_time, host_response_rate, Street, Suburb, Government_Area, Market, Country, Country_Code, Location_type,Longitude, Latitude,
            Is_Location_Exact, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location,
            review_scores_value, review_scores_rating, availability_30, availability_60, availability_90, availability_365, is_host_response, is_review_scores)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            listing_url = VALUES(listing_url),
            name = VALUES(name),
            space = VALUES(space),
            neighborhood_overview  = VALUES(neighborhood_overview),
            notes = VALUES(notes),
            transit = VALUES(transit),
            access = VALUES(access),
            interaction = VALUES(interaction),
            house_rules = VALUES(house_rules),
            property_type = VALUES(property_type),
            room_type = VALUES(room_type),
            bed_type = VALUES(bed_type),
            minimum_nights = VALUES(minimum_nights),
            maximum_nights = VALUES(maximum_nights),
            cancellation_policy = VALUES(cancellation_policy),
            last_scraped = VALUES(last_scraped),
            calendar_last_scraped = VALUES(calendar_last_scraped),
            first_review = VALUES(first_review),
            last_review = VALUES(last_review),
            accommodates = VALUES(accommodates),
            bedrooms = VALUES(bedrooms),
            beds = VALUES(beds),
            number_of_reviews = VALUES(number_of_reviews),
            bathrooms = VALUES(bathrooms),
            amenities = VALUES(amenities),
            price = VALUES(price),
            extra_people = VALUES(extra_people),
            guests_included = VALUES(guests_included),
            images = VALUES(images),
            host_id = VALUES(host_id),
            host_url = VALUES(host_url),
            host_name = VALUES(host_name),
            host_location = VALUES(host_location),
            host_about = VALUES(host_about),
            host_thumbnail_url = VALUES(host_thumbnail_url),
            host_picture_url = VALUES(host_picture_url),
            host_neighbourhood = VALUES(host_neighbourhood),
            host_is_superhost = VALUES(host_is_superhost),
            host_has_profile_pic = VALUES(host_has_profile_pic),
            host_identity_verified = VALUES(host_identity_verified),
            host_listings_count = VALUES(host_listings_count),
            host_total_listings_count = VALUES(host_total_listings_count),
            host_verifications = VALUES(host_verifications),
            host_response_time = VALUES(host_response_time),
            host_response_rate = VALUES(host_response_rate),
            Street = VALUES(Street),
            Suburb = VALUES(Suburb),
            Government_Area = VALUES(Government_Area),
            Market = VALUES(Market),
            Country = VALUES(Country),
            Country_Code = VALUES(Country_Code),
            Location_type = VALUES(Location_type),
            Longitude = VALUES(Longitude),
            Latitude = VALUES(Latitude),
            Is_Location_Exact = VALUES(Is_Location_Exact),
            review_scores_accuracy = VALUES(review_scores_accuracy),
            review_scores_cleanliness = VALUES(review_scores_cleanliness),
            review_scores_checkin = VALUES(review_scores_checkin),
            review_scores_communication = VALUES(review_scores_communication),
            review_scores_location = VALUES(review_scores_location),
            review_scores_value = VALUES(review_scores_value),
            review_scores_rating = VALUES(review_scores_rating),
            availability_30 = VALUES(availability_30),
            availability_60 = VALUES(availability_60),
            availability_90 = VALUES(availability_90),
            availability_365 = VALUES(availability_365),
            is_host_response = VALUES(is_host_response),
            is_review_scores = VALUES(is_review_scores);"""
    
    values = airbnb.values.tolist()
    mycursor.executemany(query, values)
    mydb.commit()
    return 'Successfully Inserted!'

