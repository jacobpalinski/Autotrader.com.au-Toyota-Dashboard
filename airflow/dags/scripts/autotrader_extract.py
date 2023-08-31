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

def extract_listings():
    # Scrape results from each page of used Toyota listings
    url = 'https://www.autotrader.com.au/for-sale/used/toyota'
    extracted_listings = []
    page = 1

    # Bypass cloudflare protection
    scraper = cloudscraper.create_scraper()
    
    while True and page < 10:
        try:
            response = scraper.get(url, params = {'page': page})
            if response.status_code != 200:
                raise Exception(f'Error loading page with status code {response.status_code}')
            soup = BeautifulSoup(response.text, 'html.parser')
            listings = soup.find_all('a', class_ = 'carListing')
            # Break loop if no listings as this means all pages have been scraped
            if not listings:
                break
            for listing in listings:
                price = listing.find('span', class_ = 'carListingPrice--advertisedPrice').text if listing.find('span', class_ = 'carListingPrice--advertisedPrice') != None else 'No price advertised'
                odometer = listing.find('span', class_ = 'carListing--mileage').text
                year = re.search(r'\d+',listing.find('h3', class_ = 'carListing--title').text).group()
                model = listing.find('strong', class_ = 'mmv').text.split(' ', 1)[1]
                type = listing.find('span', class_ = 'variant').text
                suburb = listing.find('div', class_ = 'carListing--location').text.split(', ', 1)[0]
                state = listing.find('div', class_ = 'carListing--location').text.split(', ', 1)[1].split()[0]
                extracted_listings.append({'price': price, 'odometer': odometer, 'year': year, 'model': model, 'type': type,
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

    current_date = datetime.now().strftime('%Y-%m-%d')

    extracted_listings_csv = StringIO()
    csv_file = csv.DictWriter(extracted_listings_csv, fieldnames = extracted_listings[0].keys())
    csv_file.writeheader()
    csv_file.writerows(extracted_listings)

    blob = bucket.blob(f'autotrader-raw-{current_date}')
    blob.upload_from_string(extracted_listings_csv.getvalue(), content_type = 'text/csv')
    
    return f'uploaded auto-trader-raw-{current_date} to autotrader-raw bucket'




