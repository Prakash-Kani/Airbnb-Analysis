# Airbnb-Analysis
___

## Problem Statement
___

This project focuses on analyzing Airbnb data using MongoDB Atlas. We aim to clean and prepare the dataset, create an interactive web app to explore listings, and visualize pricing and availability trends. The goals include establishing a MongoDB connection, developing dynamic visualizations, and building a user-friendly dashboard using tools like Tableau or Power BI. The project aims to uncover insights into pricing variations, occupancy rates, and location-based trends in an accessible and interactive manner.


## Dashboard
___
![](https://github.com/Prakash-Kani/Airbnb-Analysis/blob/main/Dashboard_image.jpg)
## Required Libraries

To run this project, you'll need to install the following Python libraries. You can do so using `pip`:

- **Streamlit:** Used to create interactive web applications in Python.
   -      pip install streamlit
- **Option Menu:** A Streamlit plugin for interactive dropdown menus.
   -      pip install streamlit-option-menu
- **Pandas:** Essential for data handling and manipulation.
   -      pip install pandas
- **NumPy:** Essential for numerical operations and data handling.
   -      pip install numpy
- **Pymongo:** Provides a Python driver for MongoDB, allowing you to work with NoSQL data.
   -      pip install pymongo
- **MySQL Connector:** Enables communication with MySQL databases for data storage and retrieval.
   -      pip install mysql-connector-python
- **Plotly Express:** A library for creating interactive data visualizations.
   -      pip install plotly
- **dotenv:** For managing environment variables.
   -      pip install python-dotenv
- **Main:** You might have a custom module; no separate installation is needed if it's part of your project.

Make sure to run these commands in your Python environment to install the required libraries.


## Example Import Statements

Here are the import statements you'll need in your Python program to use these libraries:

```
import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import datetime as dt
import numpy as np
from pymongo import MongoClient 
import mysql.connector
import os
from dotenv import load_dotenv
```

## Setting Up Environment Variables

To securely configure your database connection and credentials, create a `.env` file at the root of your project and define the following environment variables:

```
MONGODB_USER_NAME = 'Enter Your MongoDB User Name'
MONGODB_PASSWORD = 'Enter Your MongoDB Password'

MYSQL_HOST_NAME = 'Enter Your MYSQL Host Name'
MYSQL_USER_NAME = 'Enter Your MYSQL User Name'
MYSQL_PASSWORD = 'Enter Your MYSQL Password'
MYSQL_DATABASE_NAME = 'Enter Your MYSQL Database Name'
```


# ETL Process for Data Migration from MongoDB to MySQL
___

This ETL (Extract, Transform, Load) process outlines the steps involved in extracting, preprocessing, and loading data from a sample dataset in MongoDB to MySQL. Here's the breakdown:

## 1. Data Extraction from MongoDB
- The initial step is to extract data from the sample dataset stored in MongoDB. Utilizing Python scripts and pymongo, we fetch relevant information, including key details from the dataset.
## 2. Data Preprocessing and Cleaning
- Following extraction, the data undergoes preprocessing and cleaning. This involves handling missing values, removing duplicates, and ensuring data consistency to enhance its quality and usability.
## 3. Data Loading into MySQL
- After preprocessing, the data is prepared for storage in MySQL. We leverage SQLAlchemy to facilitate a smooth migration, ensuring efficient storage, retrieval, and further analysis in the MySQL database.


# Exploration and Data Analysis Process for MySQL Database
This documentation outlines the straightforward process of exploring and analyzing data from a MySQL database, employing visualizations, and creating user-friendly dashboards using Power BI and Streamlit.

## 1. Connect to MySQL Database
- **Objective:** Establish a connection to the MySQL server using the MySQL Connector, ensuring seamless access to the designated database.
- **Description:** This initial phase involves setting up a connection to the MySQL server, providing a pathway to interact with the database tables and retrieve necessary data.
## 2. Data Retrieval and Transformation
- **Objective:** Retrieve and refine data from MySQL tables using SQL queries. Transform the processed data into a DataFrame for further analysis.
- **Description:** SQL queries are applied to filter, aggregate, and shape the data for analysis. The refined dataset is then transformed into a DataFrame, laying the foundation for the subsequent analytical steps.
## 3. Dashboard Creation
- **Objective:** Develop user-friendly dashboards with both Power BI and Streamlit for interactive data exploration.
- **Description:** Two dashboards are crafted, one using Power BI and the other using Streamlit. These dashboards offer intuitive features, such as dropdown menus for selecting specific analysis questions. Users can explore data insights, presented in both tabular and graphical formats, enhancing overall data-driven decision-making.
This framework streamlines the exploration and analysis of MySQL database data, delivering user-friendly dashboards through Power BI and Streamlit for an interactive and insightful data experience.
