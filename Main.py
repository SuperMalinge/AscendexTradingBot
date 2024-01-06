# Import the necessary modules
import os
import time
import logging
import configparser
import sqlite3
import schedule
import ascendex.rest_client
import ascendex.web_socket_client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')
api_key = config['ascendex']['api_key']
secret = config['ascendex']['secret']

# Connect to the SQLite database
conn = sqlite3.connect('trades.db')
c = conn.cursor()

# Create table
c.execute('''CREATE TABLE IF NOT EXISTS trades
             (date text, buy_currency text, sell_currency text, rate real)''')

# Define the on_trade callback function
async def on_trade(trade):
    logger.info(f"New trade: {trade}")
    # Insert a row of data
    c.execute("INSERT INTO trades VALUES (?, ?, ?, ?)",
              (trade['date'], trade['buy_currency'], trade['sell_currency'], trade['rate']))
    # Save (commit) the changes
    conn.commit()

# Initialize the REST client
client = ascendex.rest_client.RestClient(GROUP_ID, api_key, secret)

try:
    # Get your balance
    balance = await client.get_balance()
    logger.info(f"Current balance: {balance}")
except ascendex.rest_client.ApiException as e:
    if e.status_code == 429:
        logger.error("Rate limit exceeded. Sleeping for a while before retrying.")
        time.sleep(60)  # Sleep for 60 seconds
    else:
        logger.error(f"An error occurred: {e}")
finally:
    # Close the client
    await client.close()

# Initialize the WebSocket client
ws = ascendex.web_socket_client.WebSocketClient(GROUP_ID, api_key, secret)

try:
    # Start the WebSocket client
    await ws.start()

    # Subscribe to trades
    await ws.subscribe("trades", symbol, on_trade)

    # Place a new order
    await ws.place_new_order(symbol, px, qty, order_type, order_side)
except ascendex.web_socket_client.ApiException as e:
    if e.status_code == 429:
        logger.error("Rate limit exceeded. Sleeping for a while before retrying.")
        time.sleep(60)  # Sleep for 60 seconds
    else:
        logger.error(f"An error occurred: {e}")
finally:
    # Close the WebSocket client
    await ws.close()

# Close the database connection
conn.close()

# Schedule the bot to run every 10 minutes
schedule.every(10).minutes.do(job)
