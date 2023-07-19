import requests
# import json
import concurrent.futures
import re
from bs4 import BeautifulSoup
import pyshorteners
import pandas as pd

def get_page_soup(url):
    HEADERS = ({'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 OPR/100.0.0.0', 'Accept-Language': 'en-US, en;q=0.5'})
    response = requests.get(url, headers=HEADERS)
    return BeautifulSoup(response.content, 'html.parser')

def process_and_save_to_json(data_list):
    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(data_list)

    # Shuffle the DataFrame rows randomly (optional)
    df = df.sample(frac=1)

    # Sort the DataFrame by 'deal_price' and 'product_name' (optional)
    # df = df.sort_values(by=['deal_price', 'product_name'])

    # Reset the index and drop the old index column
    df = df.reset_index(drop=True)

    # Drop duplicate rows from the DataFrame based on 'product_name' and 'deal_price'
    df = df.drop_duplicates(subset=['product_name', 'deal_price'], keep='first')

    # Print the final DataFrame (optional)
    print(df)
    print(df.shape)

    # Save the DataFrame to a new JSON file without the index column
    df.to_json('output_today.json', orient='records')

def shorten_url(url):
    s = pyshorteners.Shortener()
    shortened_url = s.tinyurl.short(url)
    return shortened_url

def get_category_links(base_url):
    soup = get_page_soup(base_url)
    category_links = []

    # Find all category links on the page
    category_elements = soup.find_all('a', {'class': '_6WQwDJ'})
    for element in category_elements:
        category_links.append(base_url + element['href'])

    return category_links

def scrape_product_details(url):
    all_product_details = []

    def scrape_page_products(page_url):
        soup = get_page_soup(page_url)
        product_elements = soup.find_all('div', {'class': '_4ddWXP'})
        for element in product_elements:
            product = {}
            product['product_name'] = element.find('a', {'class': 's1Q9rs'}).text
            product['deal_price'] = re.sub(r'\u20b9', '', element.find('div', {'class': '_30jeq3'}).text)
            product['img_link'] = element.find('img', class_='_396cs4')['src']
            product['purchase_link'] = shorten_url("https://www.flipkart.com" + element.find('a', class_='_2rpwqI')['href'])
            all_product_details.append(product)

    def scrape_category(category_url):
        soup = get_page_soup(category_url)
        pagination = soup.find_all('a', {'class': '_2Xp0TH'})
        max_pages = int(pagination[-1].text) if pagination else 1

        for page_num in range(1, max_pages + 1):
            page_url = f"{category_url}&page={page_num}"
            # print(page_url)
            scrape_page_products(page_url)

    base_url = 'https://www.flipkart.com/offers-store'
    category_links = get_category_links(base_url)
 
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(scrape_category, link) for link in category_links]

        for future in concurrent.futures.as_completed(futures):
            pass

    return all_product_details

def Get_json():
    base_url = 'https://www.flipkart.com/offers-store'
    all_product_details = scrape_product_details(base_url)
    # print(all_product_details)
    process_and_save_to_json(all_product_details)


    # Store the product details in a JSON file
    # with open('Today_data.json', 'w') as json_file:
    #     json.dump(all_product_details, json_file, indent=4)

if __name__ == "__main__":
    Get_json()
