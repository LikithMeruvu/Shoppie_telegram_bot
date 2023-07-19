import json
import asyncio
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
import aiohttp
import os
from urllib.parse import urlparse

# Load product details from the JSON file
def load_product_details(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
    return data

# Function to generate a valid filename from the URL
def generate_filename(url):
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)

# Asynchronous Telegram Bot
async def telegram_bot(): 
    # Set up your Telegram Bot token
    bot_token = 'Your bot token'

    # Set up the Telegram channel ID where the product details will be posted
    channel_id = 'your channel id'

    # Create a bot and dispatcher
    bot = Bot(token=bot_token)
    dp = Dispatcher(bot)

    # Load product details from the JSON file
    product_data = load_product_details('output_today.json')

    # Loop through the product data and send each product to the channel
    for i, product in enumerate(product_data):
        # Check if the maximum limit of 1000 products has been reached
        if i >= 1000:
            break

        # Extract product details from the JSON
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
        await asyncio.sleep(10)

    # Close the bot
    await bot.close()

# Run the Telegram bot
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(telegram_bot())
