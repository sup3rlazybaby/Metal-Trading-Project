import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(filename='sql_inserts_Q3.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Define SQLAlchemy engine
engine = create_engine('sqlite:///metal_commodity.db')

# Define Base class for declarative ORM
Base = declarative_base()

# Define MetalPrice ORM class
class MetalPrice(Base):
    __tablename__ = 'metal_prices'

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    metal = Column(String)
    price = Column(Float)
    macd = Column(Float)
    macd_signal = Column(Float)
    rsi = Column(Float)

# Define decorator to log SQL inserts
def log_sql_insert(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        execution_time = end_time - start_time
        logging.info(f"SQL insert executed in {execution_time.total_seconds()} seconds")
        return result
    return wrapper

# Function to calculate MACD for a series of prices
def calculate_macd(prices, slow_period=26, fast_period=12, signal_period=9):
    slow_ema = prices.ewm(span=slow_period).mean()
    fast_ema = prices.ewm(span=fast_period).mean()
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal_period).mean()
    return macd_line, signal_line

# Function to calculate RSI for a series of prices
def calculate_rsi(prices, window=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Function to plot MACD, RSI, and price for each metal
def plot_macd_rsi_price(df, metals):    

    for metal in metals:
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 10), gridspec_kw={'height_ratios': [7, 2, 2]})

        # Plot price
        ax1.plot(df['Dates'], df[metal], label='Price')
        ax1.set_title(f'{metal} Analysis')
        ax1.set_ylabel('Price')
        ax1.legend()
        ax1.grid(True)

        # Plot MACD and MACD Signal
        ax2.plot(df['Dates'], df[f'{metal}_macd'], label='MACD')
        ax2.plot(df['Dates'], df[f'{metal}_macd_signal'], label='MACD Signal')
        ax2.set_ylabel('MACD')
        ax2.legend()
        ax2.grid(True)

        # Plot RSI
        ax3.plot(df['Dates'], df[f'{metal}_rsi'], label='RSI', color='orange')
        ax3.set_ylabel('RSI')
        ax3.legend()
        ax3.grid(True)

        plt.xlabel('Date')
        plt.tight_layout()
        plt.show()
        # plt.savefig(f'{metal}_analysis.png')
        # plt.close()

# Function to read CSV file and calculate MACD and RSI
def calculate_macd_rsi(csv_file):
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file)

    # Convert 'Dates' column to datetime
    df['Dates'] = pd.to_datetime(df['Dates'])

    # Extract metal column names
    metals = df.columns[1:]

    # Iterate over metal columns and calculate MACD, RSI
    for metal in metals:
        prices = df[metal]
        macd_line, macd_signal = calculate_macd(prices)
        rsi = calculate_rsi(prices)
        df[f'{metal}_macd'] = macd_line
        df[f'{metal}_macd_signal'] = macd_signal
        df[f'{metal}_rsi'] = rsi

    return df, metals

# Function to populate SQL table with calculated data
@log_sql_insert
def populate_sql_table(df, metals):
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Iterate over DataFrame rows and insert into SQL table
    for index, row in df.iterrows():
        date = row['Dates']
        for metal in metals:
            price = row[metal]
            macd = row[f'{metal}_macd']
            macd_signal = row[f'{metal}_macd_signal']
            rsi = row[f'{metal}_rsi']

            metal_price = MetalPrice(date=date, metal=metal, price=price,
                                     macd=macd, macd_signal=macd_signal, rsi=rsi)
            session.add(metal_price)

    # Commit changes
    session.commit()

# Read CSV file and calculate MACD, RSI
csv_file = 'MarketData_filtered.csv'
df, metals = calculate_macd_rsi(csv_file)

# Plot MACD, RSI, and price for each metal
# plot_macd_rsi_price(df, metals)

# Populate SQL table with calculated data
# populate_sql_table(df, metals)

# Check SQL table
Session = sessionmaker(bind=engine)
session = Session()
# Print updated MetalPrice table 
prices = session.query(MetalPrice).all()
for price in prices:
    print(f"ID: {price.id}, Metal: {price.metal}, Date: {price.date}, Price: {price.price}, MACD: {price.macd}, MACD_signal: {price.macd_signal}, RSI: {price.rsi}")