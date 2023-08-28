from bs4 import BeautifulSoup
from cloudscraper

def extract_listings():
    url = 'https://www.autotrader.com.au/for-sale/used/toyota'
    results = []

    scraper = cloudscraper.create_scraper()
    
    try:
        response = scraper.get(url)
        if response.status_code != 200:
            raise Exception(f'Error loading page with status code {response.status_code}')
        soup = BeautifulSoup(response.text, 'html.parser')
        listings = soup.find_all('a', class_ = 'carListing')
        for listing in listings:
            price = listing.find('span', class_ = 'carListingPrice--advertisedPrice').text
            odometer = listing.find('span', class_ = 'carListing--mileage').text
            year = listing.find('h3', class_ = 'carListing--title').text
            model = listing.find('strong', class_ = 'mmv').text.split(' ', 1)[1]
            type = listing.find('span', class_ = 'variant').text
            suburb = listing.find('div', class_ = 'carListing--location').text.split(', ', 1)[0]
            state = listing.find('div', class_ = 'carListing--location').text.split(', ', 1)[1].split()[0]
            results.append({'price': price, 'odometer': odometer, 'year': year, 'model': model, 'type': type,
            'suburb': suburb, 'state': state})
    except Exception as error:
        return error
    
    return results

