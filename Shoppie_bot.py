import asyncio
import requests
import concurrent.futures
import re
from bs4 import BeautifulSoup
import pyshorteners
from aiogram import Bot, types, Dispatcher
import aiohttp
import os
from urllib.parse import urlparse
import pandas as pd

# Function to generate a valid filename from the URL
def generate_filename(url):
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)

# Function to get the page soup
def get_page_soup(url):
    HEADERS = ({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 OPR/100.0.0.0',
        'Accept-Language': 'en-US, en;q=0.5'
    })
    response = requests.get(url, headers=HEADERS)
    return BeautifulSoup(response.content, 'html.parser')

# Function to shorten a URL
def shorten_url(url):
    s = pyshorteners.Shortener()
    shortened_url = s.tinyurl.short(url)
    return shortened_url

# Function to get category links from the base URL
def get_category_links(base_url):
    soup = get_page_soup(base_url)
    category_links = []

    # Find all category links on the page
    category_elements = soup.find_all('a', {'class': '_6WQwDJ'})
    for element in category_elements:
        category_links.append(base_url + element['href'])

    return category_links

# Function to scrape product details
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
            scrape_page_products(page_url)

    base_url = 'https://www.flipkart.com/offers-store'
    category_links = get_category_links(base_url)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(scrape_category, link) for link in category_links]

        for future in concurrent.futures.as_completed(futures):
            pass

    # Convert the list of dictionaries to a pandas DataFrame
    df = pd.DataFrame(all_product_details)

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

    return df

# Asynchronous Telegram Bot
async def telegram_bot(all_product_details):
    # Set up your Telegram Bot token
    bot_token = 'your bot token'

    # Set up the Telegram channel ID where the product details will be posted
    channel_id = 'your channel id'

    # Create a bot and dispatcher
    bot = Bot(token=bot_token)
    dp = Dispatcher(bot)

    # Loop through the product data and send each product to the channel
    for i, product in enumerate(all_product_details):
        # Check if the maximum limit of 1000 products has been reached
        if i >= 1000:
            break

        # Extract product details
        product_title = product['product_name']
        deal_price = product['deal_price']
        img_url = product['img_link']
        product_url = product['purchase_link']

        # Prepare the caption message with the product details
        caption = f"<b>Product:</b> {product_title}\n\n<b>Deal Price:</b> {deal_price}✅✅✅\n<b>Purchase 💵:</b> {product_url}"

        # Generate a valid filename for the image
        filename = generate_filename(img_url)

        # Download the image locally
        async with aiohttp.ClientSession() as session:
            async with session.get(img_url) as response:
                if response.status == 200:
                    with open(filename, 'wb') as f:
                        f.write(await response.read())

        # Send the image with the caption to the Telegram channel
        with open(filename, 'rb') as photo_file:
            await bot.send_photo(chat_id=channel_id, photo=photo_file, caption=caption, parse_mode='HTML')

        # Delete the downloaded image file
        os.remove(filename)

        # Wait for 10 seconds before sending the next product
        await asyncio.sleep(3)

    # Close the bot
    await bot.close()

def main():
    base_url = 'https://www.flipkart.com/offers-store'
    df = scrape_product_details(base_url)
    all_product_details = df.to_dict(orient='records')
    asyncio.run(telegram_bot(all_product_details))

if __name__ == "__main__":
    main()