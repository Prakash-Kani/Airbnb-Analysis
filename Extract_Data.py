from pymongo import MongoClient
import pandas as pd
import numpy as np
import datetime as dt

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

username =os.getenv("MONGODB_USER_NAME")
password = os.getenv('MONGODB_PASSWORD')


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
        df['last_scraped'] = pd.to_datetime(df['last_scraped'])
        df['calendar_last_scraped'] = pd.to_datetime(df['calendar_last_scraped'])
        df['first_review'] = pd.to_datetime(df['first_review'], format = '%Y-%m-%d').dt.date
        df['last_review'] = pd.to_datetime(df['last_review'], format = '%Y-%m-%d').dt.date
        df['last_scraped'] = pd.to_datetime(df['last_scraped'])
        df['last_scraped'] = pd.to_datetime(df['last_scraped'])

        self.df = df

        return self.df
    
    def Host_Data(self):
        host=[]
        query ={}
        projection = {'host':True,'_id':True}
        for i in self.collection.find(query, projection):
            i['host']['_id'] = i['_id']

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



def Preprecessing(airbnb):

    duplicates = airbnb.duplicated(subset = '_id', keep = 'first')
    if not duplicates.empty:
        airbnb.drop(airbnb[duplicates].index, inplace = True)

    inconsistent_days=airbnb[(airbnb['availability_30'] <0 ) & (airbnb['availability_30'] >30)]
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

    inconsistent_dates = airbnb[(airbnb['last_review']> dt.date.today()) | (airbnb['first_review']> dt.date.today())]
    if not inconsistent_dates.empty:
        airbnb.drop(inconsistent_dates.index, inplace = True)

    inconsistent_dates = airbnb[airbnb['first_review'] >airbnb['last_review']]
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

    airbnb=Preprecessing(airbnb)

    return airbnb


