import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import time
import random
import pandas as pd
import numpy as np
import json

# 1. WEB_SCRAPPING LIST OF PROPERTY NUMBERS

# 1.1 NUMBER OF PAGES OF RESIDENTIAL LISTING
# 1.2 LIST OF RESIDENTIAL PROPERTY NUMBERS

def number_of_pages_listing():
    base_url = 'https://www.28hse.com/en/rent/residential'
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    page_numbers = [a.get_text(strip=True) for a in soup.find_all('a', class_='item') if a.get_text(strip=True).isdigit()]
    return int(page_numbers[-1])

def extract_property_nums_from_soup(soup):
    listings = soup.find_all('a', class_='detail_page')
    # Extract listing numbers
    listing_numbers = []
    for listing in listings:
        # Extract from 'attr1'
        listing_number_attr = listing.get('attr1')
        # Extract from 'href'
        href = listing.get('href')
        if href:
            # Parse the listing number from the URL
            listing_number_href = href.split('-')[-1]
            listing_numbers.append(listing_number_href)
    
        # Append the extracted number (ensure no duplicates)
        if listing_number_attr and listing_number_attr not in listing_numbers:
            listing_numbers.append(listing_number_attr)
    
    return list(set(listing_numbers))


def list_of_properties_scrapping():
    # Base URL template
    base_url = "https://www.28hse.com/en/rent/residential?page={page}&sortBy=default&search_words_thing=default&buyRent=rent&propertyDoSearchVersion=2.0"
    
    # Function to fetch and parse a page with retries
    def fetch_page(page, max_retries=2):
        url = base_url.format(page=page)
        
        for attempt in range(max_retries + 1):  # Try up to max_retries times
            try:
                print(f"Fetching page {page}, attempt {attempt + 1}...")
                response = requests.get(url, timeout=10)  # Set timeout to prevent hanging
                
                response.raise_for_status()  # Raise an exception for HTTP errors
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    return extract_property_nums_from_soup(soup)  # Extract property numbers
                
                print(f"Failed to fetch page {page} with status code {response.status_code}")
            except requests.RequestException as e:
                print(f"Error fetching page {page} (attempt {attempt + 1}): {e}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)  # Exponential backoff before retrying
            
        print(f"Giving up on page {page} after {max_retries + 1} attempts.")
        return []  # Return empty list if all attempts fail

    # Initialize a list to collect property numbers
    property_numbers = []
    
    # Get total pages dynamically
    total_pages = number_of_pages_listing()
    
    # Use ThreadPoolExecutor to fetch pages in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit tasks for all pages
        future_to_page = {executor.submit(fetch_page, page): page for page in range(1, total_pages + 1)}
    
        # Process results as they complete
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                # Collect the property numbers from the completed task
                property_numbers.extend(future.result())
            except Exception as e:
                print(f"Error processing page {page}: {e}")
    
    # Print the total number of properties collected
    print(f"Collected {len(property_numbers)} property numbers")
    return property_numbers


# 2. WEB_SCRAPPING PROPERTIES DETAILS

# 2.1 NUMBER OF PAGES OF RESIDENTIAL LISTING
# 2.2 LIST OF RESIDENTIAL PROPERTY NUMBERS


def extract_estate_info(soup):
    estate_info = soup.find('div', class_='estateInfo')
    num_units = 'Not available'
    if estate_info:
        num_units = estate_info.find('td', text='Unit Desc')
        num_units = num_units.find_next('td').get_text(strip=True) if num_units else 'Not available'
    
    floor_zone = soup.find('td', text='Floor zone')
    floor_zone = floor_zone.find_next('td').find('div', class_='pairValue').get_text(strip=True) if floor_zone else 'Not available'

    return {
        'num_units': num_units,
        'floor_zone': floor_zone
    }

def extract_room_bathroom_info(soup):
    """
    Extract room and bathroom information from the page if available.

    Parameters:
        soup (BeautifulSoup): The BeautifulSoup object containing the page content.

    Returns:
        tuple: A tuple containing the number of rooms and bathrooms, or 'Not available' if not found.
    """
    room_bathroom = soup.find('td', text='Room and Bathroom')
    
    if room_bathroom:
        room_bathroom_value = room_bathroom.find_next('td').find('div', class_='pairValue')
        room_bathroom_text = room_bathroom_value.get_text(strip=True, separator=" ") if room_bathroom_value else 'Not available'
        
        try:
            room_info, bathroom_info = room_bathroom_text.split(' ')[0], room_bathroom_text.split(' ')[2] if ' ' in room_bathroom_text else ('Not available', 'Not available')
        except:
            room_info, bathroom_info = 'Not available', 'Not available'
    else:
        room_info, bathroom_info = 'Not available', 'Not available'

    return room_info, bathroom_info

def extract_property_type(soup):
    property_type = 'Not available'
    if 'Office Rental' in soup.get_text():
        property_type = 'Office'
    elif 'Rent Property' in soup.get_text():
        property_type = 'Property'
    return property_type

def extract_url_history(data):
    """
    Extracts the 'url_history' from the given data dictionary.

    Parameters:
        data (dict): The JSON data containing 'potentialAction'.

    Returns:
        str or np.nan: The extracted URL if available, otherwise NaN.
    """
    try:
        potential_action = data.get('potentialAction', [])
        
        if isinstance(potential_action, list) and potential_action:
            target = potential_action[0].get('target', {})
            
            if isinstance(target, dict):  # Ensure target is a dictionary
                return target.get('urlTemplate', '') + '/transaction/rent'
        
        return np.nan  # Return NaN if conditions are not met

    except Exception as e:
        print(f"Error extracting url_history: {e}")
        return np.nan

def get_property_details_from_json(data_dict, json_data, soup):
    data = json.loads(json_data)
    main_entity = data.get('mainEntity', {})

    # Extracting relevant information from the JSON
    data_dict['date_published'] = data.get('datePublished', 'Not available')
    data_dict['price'] = data.get('offers', {}).get('price', 'Not available')
    data_dict['url_history'] = extract_url_history(data)
    if isinstance(main_entity,list):
        return get_property_details_from_soup(data_dict, soup)
    else:
        data_dict['type'] = main_entity.get('@type', 'Not available')
        data_dict['latitude'] = main_entity.get('geo', {}).get('latitude', 'Not available')
        data_dict['longitude'] = main_entity.get('geo', {}).get('longitude', 'Not available')
        data_dict['description'] = main_entity.get('description', 'Not available')
        data_dict['floor_size'] = main_entity.get('floorSize', {}).get('value', 'Not available')
        data_dict['floor_size_unit'] = main_entity.get('floorSize', {}).get('unitCode', 'Not available')
        data_dict['address'] = main_entity.get('address', 'Not available')
        data_dict['number_of_rooms'] = main_entity.get('numberOfRooms', 'Not available')
        data_dict['name'] = main_entity.get('name', 'Not available')
        return data_dict

def get_property_details_from_soup(data_dict, soup):
    # If no JSON data is found, use classic scraping methods to extract data
    data_dict['name'] = soup.find('h1', {'class': 'propertyTitle'}).get_text(strip=True) if soup.find('h1', {'class': 'propertyTitle'}) else 'Not available'
    data_dict['address'] = soup.find('td', text='Address').find_next('td').find('div', class_='pairValue').get_text(strip=True) if soup.find('td', text='Address') else 'Address not available'
    data_dict['price'] = soup.find('td', text='Monthly Rental').find_next('td').find('div', class_='pairValue price green').get_text(strip=True) if soup.find('td', text='Monthly Rental') else 'Price not available'
    data_dict['floor_size'] = soup.find('td', text='Saleable Area').find_next('td').find('div', class_='pairValue').get_text(strip=True) if soup.find('td', text='Saleable Area') else 'Not available'
    data_dict['unit_price'] = soup.find('div', class_='pairSubValue').get_text(strip=True) if soup.find('div', class_='pairSubValue') else 'Not available'
    return data_dict

def get_property_details(prop_num):
    url = f'https://www.28hse.com/en/rent/residential/property-{prop_num}'
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch the property: {prop_num}")
        print(url)
        return {}
    
    soup = BeautifulSoup(response.text, 'html.parser')
    if soup is None:
        return {}
    
    property_details = {}
    json_data = soup.find('script', type='application/ld+json').string if soup.find('script', type='application/ld+json') else None
    if json_data:
        property_details = get_property_details_from_json(property_details, json_data, soup)
    else:
        try:
            property_details = get_property_details_from_soup(property_details,json_data)
        except:
            print(f"Failed to fetch the property: {prop_num}")
            print(url)
            return {}
            
    # Use the optimized functions to extract estate and room details
    estate_details = extract_estate_info(soup)
    property_details.update(estate_details)

    room_info, bathroom_info = extract_room_bathroom_info(soup)
    property_details['number_of_rooms'] = room_info
    property_details['number_of_bathrooms'] = bathroom_info

    # Extract property type based on the presence of keywords
    property_type = extract_property_type(soup)
    property_details['property_type'] = property_type
    property_details['property_number'] = prop_num
    property_details['url'] = url

    return property_details

def get_properties_dataframe_parallel(property_numbers):
    start_time = time.time()
    properties_data = []

    # Use ThreadPoolExecutor to scrape multiple pages concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_property_details, prop_num) for prop_num in property_numbers]
        for future in concurrent.futures.as_completed(futures):
            properties_data.append(future.result())

    # Convert to DataFrame
    df = pd.DataFrame(properties_data)

    # End timer
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time:.4f} seconds")
    
    return df


# 3. WEB_SCRAPPING LEASING HISTORY

# 3.1 NUMBER OF PAGES OF ADDRESS HISTORICAL LEASING
# 3.2 SCRAPPING 

def number_of_pages_building(base_url):
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    page_numbers = [a.get_text(strip=True) for a in soup.find_all('a', class_='item') if a.get_text(strip=True).isdigit()]
    return int(page_numbers[-1])


def parse_flats_from_page(html_content):
    """Parses a single page of rental listings and extracts structured data."""
    soup = BeautifulSoup(html_content, "html.parser")
    flats = []

    for item in soup.find_all("div", class_="item", attrs={"unit-id": True}):
        # Extract Flat Name
        flat_name_tag = item.find("a", class_="detail_page")
        flat_name = flat_name_tag.text.strip() if flat_name_tag else "N/A"

        # Extract Size (ft²)
        description = item.find("div", class_="description")
        size_text = description.text.strip().split()[0].replace("ft²", "").strip() if description else "N/A"
        size = int(size_text) if size_text.isdigit() else "N/A"

        # Extract Unit Price
        unit_price_tag = item.find("span", class_="unit_price")
        unit_price = unit_price_tag.text.strip().replace("$", "") if unit_price_tag else "N/A"

        # Extract Lease Date
        date_tag = item.find("i", class_="calendar alternate icon")
        date_label = date_tag.find_parent("div") if date_tag else None
        date = date_label.text.strip() if date_label else "N/A"

        # Extract Number of Rooms
        rooms_tag = item.find("div", class_="ui label", string=lambda x: x and "Rooms" in x)
        num_rooms = rooms_tag.text.replace("Rooms", "").strip() if rooms_tag else "N/A"

        # Extract Leased Price
        price_tag = item.find("div", class_="transaction_detail_price_rent")
        leased_price_text = price_tag.find("div", style="float: right;").text.strip() if price_tag else "N/A"
        leased_price = leased_price_text.replace("Leased", "").replace("HKD$", "").replace(",", "").strip()

        # Store the extracted data
        flats.append({
            "Flat Name": flat_name,
            "Size (ft²)": size,
            "Unit Price (HKD/ft²)": unit_price,
            "Lease Date": date,
            "Rooms": num_rooms,
            "Leased Price (HKD)": leased_price
        })

    return flats

def fetch_page_building(url):
    """Fetches a single page and returns its HTML content."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    return None

def scrape_all_pages_building(base_url, total_pages, max_workers=5):
    """Scrapes multiple pages in parallel and aggregates data."""
    urls = [f"{base_url}/page-{i}" for i in range(1, total_pages + 1)]
    flats_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(fetch_page_building, urls)

    for html_content in results:
        if html_content:
            flats_data.extend(parse_flats_from_page(html_content))

    return flats_data


def fetch_page_with_retries(url, max_retries=3):
    """Fetch a page with retry logic and exponential backoff."""
    attempt = 0
    while attempt < max_retries:
        try:
            html_content = fetch_page_building(url)
            if html_content:
                return html_content
            else:
                raise ValueError("Empty page response")
        except Exception as e:
            print(f"Error fetching {url} (attempt {attempt+1}): {e}")
            attempt += 1
            time.sleep(2 ** attempt + random.uniform(0, 1))  # Exponential backoff

    print(f"Failed to fetch {url} after {max_retries} attempts")
    return None  # Return None if all attempts fail

def scrape_all_pages_building(base_url, total_pages, max_workers=5):
    """Scrapes multiple pages in parallel and aggregates data with error handling."""
    urls = [f"{base_url}/page-{i}" for i in range(1, total_pages + 1)]
    flats_data = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_page_with_retries, url): url for url in urls}

        for future in as_completed(future_to_url):
            try:
                html_content = future.result()
                if html_content:
                    flats_data.extend(parse_flats_from_page(html_content))
            except Exception as e:
                print(f"Error processing {future_to_url[future]}: {e}")

    return flats_data


def scrape_building_with_metadata(url_history, property_df):
    """Scrape lease history for a given URL with associated metadata."""
    try:
        total_pages = number_of_pages_building(url_history)
        address = property_df.loc[property_df['url_history'] == url_history, 'address'].iloc[0]
        latitude = property_df.loc[property_df['url_history'] == url_history, 'latitude'].iloc[0]
        longitude = property_df.loc[property_df['url_history'] == url_history, 'longitude'].iloc[0]

        flats_data = scrape_all_pages_building(url_history, total_pages)
        
        df_building = pd.DataFrame(flats_data)
        df_building['address'] = address
        df_building['latitude'] = latitude
        df_building['longitude'] = longitude

        return df_building
    except Exception as e:
        print(f"Error with {url_history}: {e}")
        return None
    
def get_lease_history(property_df):
    start_time = time.time()
    list_of_url_history = list(set(property_df.dropna(subset=['url_history'])['url_history']))
    flats_data=[]
    df_history=pd.DataFrame([])
    for url_history in list_of_url_history:
        try :
            total_pages = number_of_pages_building(url_history)
            address = property_df[lambda x : x.url_history==url_history].iloc[0]['address']
            latitude = property_df[lambda x : x.url_history==url_history].iloc[0]['latitude']
            longitude = property_df[lambda x : x.url_history==url_history].iloc[0]['longitude']
            flats_data = scrape_all_pages_building(url_history, total_pages)
            df_building = pd.DataFrame(flats_data)
            df_building['address']=address
            df_building['latitude']=latitude
            df_building['longitude']=longitude
            df_history = pd.concat([df_history,df_building])
        except:
            print(f'error with {url_history}')

    # End timer
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution Time: {execution_time:.4f} seconds")
    return df_history

def get_lease_history_parallel(list_of_url_history, property_df, max_workers=5):
    """Fetch lease history in parallel and aggregate results."""
    df_history = pd.DataFrame([])

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(scrape_building_with_metadata, url, property_df): url for url in list_of_url_history}

        for future in as_completed(future_to_url):
            result = future.result()
            if result is not None:
                df_history = pd.concat([df_history, result], ignore_index=True)

    return df_history

