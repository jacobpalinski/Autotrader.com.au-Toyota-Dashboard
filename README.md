## Overview
The Autotrader Toyota Dashboard is a dashboard that visualises the last 3 months of used Toyota listing data from Autotrader.com.au. 
The dashboard enables users to identify which Toyota represents the best value money based on their needs. Nationwide, statewide and 
model specific anaylsis are included in dashboard. 

For the project a ELT data pipeline was constructed with Apache Airflow, BigQuery, DBT and Docker. Autotrader.com.au Toyota listings data is the source and it is integrated with an Australian Postcodes csv file.
The Australian Postcodes csv file contains latitude and longitude information for every Australian suburb. 

Dashboard was built using Looker and pipeline was deployed on GCE (Google Compute Engine).

Dashboard can be found [here](https://lookerstudio.google.com/u/0/reporting/3229c9a4-59a5-47cc-a95a-e69214142d2d/page/cMskD)

All tools and technologies used: Python, SQL, GCS (Google Cloud Storage), Apache Airflow, Google BigQuery, DBT (Data Build Tool), Docker, Great Expectations.

## Data Pipeline
### Architecture
![image](https://github.com/jacobpalinski/Autotrader.com.au-Toyota-Dashboard/assets/64313786/b1bdf4ba-c829-4690-ac48-eff319122adb)

### Extraction
Listing data is extracted from each successive page from the following initial url https://www.autotrader.com.au/for-sale/toyota. 
The following data is extracted for each listing:
- date
- price
- odometer
- year
- car model
- type
- suburb
- state
Extracted data is stored in GCS Bucket.

Extraction script can be found in `airflow-astro/dags/scripts/autotrader_extract.py` file.

### Loading and Transformation
Raw data from autotrader.com.au and the Australian Postcodes csv are then loaded from GCS bucket into BigQuery data warehouse raw layer. 
From the raw layer to the staging layer null and empty values are removed, as well string manipulations and type conversions are made for scraped data.
Additionally only state, latitude and longitude values for suburbs in scraped listings are extracted from Australian Postcodes. In the resulting 
locations_staging table no duplicates are produced. 

The tables used in the staging layer have the following schema:

![image](https://github.com/jacobpalinski/Autotrader.com.au-Toyota-Dashboard/assets/64313786/aaece8f6-9d2c-4645-97ab-0290cedf4acf)

From the staging layer to the transformed layer the listings_staging and locations_staging are transformed to form a data warehouse for analytics. 
An inceremental update pattern is used for the datawarehouse meaning that only listings, locations, and car models that don't already exist in warehouse are added. 

Analytics warehouse has the following Dimensional Model structure:

![image](https://github.com/jacobpalinski/Autotrader.com.au-Toyota-Dashboard/assets/64313786/c297915a-bf41-4f51-be36-18ba36711806)

Data in the data warehouse is then to create OBT for Looker dashboard. 
In this transformation processs, only the last 3 months worth of listings data is used and averages for each year + model combination are calculated at the both the national and the state level. 

Analytics Layer (OBT) has the following schema:

![image](https://github.com/jacobpalinski/Autotrader.com.au-Toyota-Dashboard/assets/64313786/3484b2e4-223c-4adc-bc9c-64d0ecad8d89)

DBT unit testing along with the Great Expectations plugin are used for testing all transformations.

DBT transformations and tests can be found in the `airflow-astro/include/dbt/models` folder.

### Orchestration
Data pipeline is orchestrated using Apache Airflow. Pipeline runs batch process daily.

DAG structure:
![image](https://github.com/jacobpalinski/Autotrader.com.au-Toyota-Dashboard/assets/64313786/637746be-59bb-4cf2-b604-8f0f0cadff53)

DAG code can be found in the `airflow-astro/dags/autotrader_etl.py` file. 

## Dashboard

The Looker dashboard contains 3 tabs namely National, State and Model. 

The National tab contains the following visualisations:
- Price Distribution by Model (table): minimum, average, median and max price by model
- Top 5 Listings (bar chart): top 5 models by listing frequency
- Price vs Odometer Reading by Year (scatter plot): median odometer reading and average price by year
- % Listings by State (bar chart): percentage of total listings by state based on listing frequency

The State tab has the following visuals while are filterable by state and year:
- Models Listed (table): number of listings and average price by model
- Average Price Relative to National Average (bar chart): average price of all models listed in state relative to national average for model
- Price + Odometer Reading By Suburb (bubble map): median price and average price by suburb in state
- Odometer Reading By Model (bar chart): median odometer reading by model sorted in ascending order

The Model tab is filterable by model, year and odometer range and contains the following visualisations:
- National Average (scorecard): national average based on filters
- Listings (table): table containing infromation about number of listings, average price, median odometer reading, and price difference vs national average for a given state, suburb, type combination
- Price vs Odometer Reading By Type (scatterplot): median odometer reading and average price by model type
- Price Trend (line graph): average price over time by state



































