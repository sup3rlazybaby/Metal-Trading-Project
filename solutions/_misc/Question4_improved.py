import pandas as pd
import asyncio
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from datetime import datetime
import logging
from functools import wraps

# Configure logging
logging.basicConfig(filename='sql_inserts_Q4.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Define SQLAlchemy engine, specifying additional aiosqlite driver 
engine = create_async_engine('sqlite+aiosqlite:///metal_commodity_q4.db')  # Use future=True for asyncio compatibility

# Define Base class for declarative ORM
class Base(DeclarativeBase):
    pass

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

# Define decorator to log SQL operations
def log_sql(func):
    @wraps(func)    # Preserve metadata of original function, helps debugging
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        end_time = datetime.now()
        execution_time = end_time - start_time
        logging.info(f"SQL operation {func.__name__} executed in {execution_time.total_seconds()} seconds (asynchronous)")
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

# Async function to process CSV and insert data into database using SQLAlchemy
@log_sql
async def process_csv_async(csv_file):
    df = pd.read_csv(csv_file)
    df['Dates'] = pd.to_datetime(df['Dates'])

    # Create an async session
    async_session = async_sessionmaker(bind=engine, expire_on_commit=False)    # SQLAlchemy version > 2.0
    
    async with async_session() as session:
        for index, row in df.iterrows():
            date = row['Dates']
            for metal in df.columns[1:]:
                price = row[metal]
                prices = df[metal]
                macd_line, macd_signal = calculate_macd(prices)
                rsi = calculate_rsi(prices)

                # Create MetalPrice instance
                metal_price = MetalPrice(date=date, metal=metal, price=price,
                                         macd=macd_line.iloc[index], macd_signal=macd_signal.iloc[index], rsi=rsi.iloc[index])

                # Add the MetalPrice instance to the session
                session.add(metal_price)
                
        # Commit changes
        await session.commit()

    # Close and clean-up pooled connections.
    await engine.dispose()

# Run the async processing function
async def main():
    await process_csv_async('MarketData_filtered.csv')

# Execute the asyncio event loop
asyncio.run(main())

# Check SQL table
# async def main2():
#     # Create an async session
#     async_session = async_sessionmaker(bind=engine, expire_on_commit=False)   
    
#     async with async_session() as session:
#         prices = session.query(MetalPrice).all()
#         for price in prices:
#             print(f"ID: {price.id}, Metal: {price.metal}, Date: {price.date}, Price: {price.price}, MACD: {price.macd}, MACD_signal: {price.macd_signal}, RSI: {price.rsi}")

# asyncio.run(main2())

# engine2 = create_engine('sqlite:///metal_commodity_q4_2.db')
# Session = sessionmaker(bind=engine2)
# session = Session()
# # Print updated MetalPrice table 
# prices = session.query(MetalPrice).all()
# for price in prices:
#     print(f"ID: {price.id}, Metal: {price.metal}, Date: {price.date}, Price: {price.price}, MACD: {price.macd}, MACD_signal: {price.macd_signal}, RSI: {price.rsi}")