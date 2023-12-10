from bs4 import BeautifulSoup
import cloudscraper
import re
import os
from google.oauth2 import service_account
from google.cloud import storage
from io import StringIO
import csv
from datetime import datetime
from dotenv import load_dotenv
import json
from time import sleep

current_date = datetime.now().strftime('%Y-%m-%d')

def extract_listings(current_date: str) -> str:
    # Scrape results from each page of used Toyota listings
    url = 'https://www.autotrader.com.au/for-sale/used/toyota'
    extracted_listings = []
    page = 1

    # Bypass cloudflare protection
    scraper = cloudscraper.create_scraper()
    
    while True:
        try:
            print(page)
            response = scraper.get(url, params = {'page': page})
            if response.status_code != 200:
                sleep(7) # Extend sleep for future
                #raise Exception(f'Error loading page with status code {response.status_code}')
            soup = BeautifulSoup(response.text, 'html.parser')
            listings = soup.find_all('a', class_ = 'carListing')
            # Break loop if no listings as this means all pages have been scraped
            if not listings:
                break
            for listing in listings:
                # Data to be extracted from listing display
                price = listing.find('span', class_ = 'carListingPrice--advertisedPrice').text if listing.find('span', class_ = 'carListingPrice--advertisedPrice') != None else None
                odometer = listing.find('span', class_ = 'carListing--mileage').text
                year = re.search(r'\d+',listing.find('h3', class_ = 'carListing--title').text).group()
                car_model = listing.find('strong', class_ = 'mmv').text.split(' ', 1)[1]
                type = listing.find('span', class_ = 'variant').text
                suburb = listing.find('div', class_ = 'carListing--location').text.split(', ', 1)[0]
                state = listing.find('div', class_ = 'carListing--location').text.split(', ', 1)[1].split()[0]
                extracted_listings.append({'date': current_date, 'price': price, 'odometer': odometer, 'year': year, 'car_model': car_model, 'type': type,
                'suburb': suburb, 'state': state})
            page += 1
        except Exception as error:
            return error
    
    # Connect to gcp and upload listings in csv file
    load_dotenv()
    credentials_dict = {
        'type': 'service_account',
        'project_id': os.environ.get('PROJECT_ID'),
        'private_key_id': os.environ.get('PRIVATE_KEY_ID'),
        'private_key': os.environ.get('PRIVATE_KEY'),
        'client_email': os.environ.get('CLIENT_EMAIL'),
        'client_id': os.environ.get('CLIENT_ID'),
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': os.environ.get('CLIENT_X509_CERT_URL'),
        'universe_domain': 'googleapis.com'
    }

    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    client = storage.Client(project = 'autotrader-toyota-dashboard', credentials = credentials)
    bucket = client.bucket('autotrader-raw')

    # Convert list of dictionaries to NDJSON format
    extracted_listings_ndjson = ''
    for listing in extracted_listings:
        extracted_listings_ndjson += json.dumps(listing) + '\n'
    blob = bucket.blob(f'autotrader-raw-{current_date}.json')
    blob.upload_from_string(extracted_listings_ndjson, content_type = 'application/json')
    return f'uploaded auto-trader-raw-{current_date}.json to autotrader-raw bucket'




