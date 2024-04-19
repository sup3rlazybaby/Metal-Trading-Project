import pandas as pd
import asyncio
from sqlalchemy import select, Column, Integer, String, Float, Date
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime
import logging
from functools import wraps

# Configure logging
logging.basicConfig(filename='sql_inserts_Q4.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Define SQLAlchemy engine, specifying additional aiosqlite driver 
engine = create_async_engine('sqlite+aiosqlite:///metal_commodity_Q4_TEST.db')  # Use future=True for asyncio compatibility

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

    return (df, metals)

# Function to populate SQL table with calculated data
@log_sql
async def populate_sql_table(df, metals):
    # Create session
    async_session = async_sessionmaker(bind=engine, expire_on_commit=False)    # SQLAlchemy version > 2.0
    async with async_session() as session:

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
        await session.commit()
        
    # Close and clean-up pooled connections.
    # await engine.dispose()

# Run the async processing function
async def main_write():
    # Read CSV file and calculate MACD, RSI
    csv_file = 'MarketData_filtered.csv'
    df, metals =  calculate_macd_rsi(csv_file)             
    
    # Populate SQL table with calculated data
    await populate_sql_table(df, metals)

# Execute the asyncio event loop
asyncio.run(main_write())



# Define async session
async_session = async_sessionmaker(bind=engine, expire_on_commit=False)    

# Async function to read data from the database
async def read_data(query):
    async with async_session() as session:
        result = await session.execute(query)
        data = result.fetchall()        
        return data

# Async function to perform concurrent database reads 5 times
async def concurrent_reads():
    queries = [
        select(MetalPrice).where(MetalPrice.metal == 'COPPER'),     # Example
        select(MetalPrice).where(MetalPrice.metal == 'ZINC'),       # Example
        select(MetalPrice).where(MetalPrice.date >= '2022-01-01'),  # Example
        select(MetalPrice).where(MetalPrice.rsi >= 40),             # Example
        select(MetalPrice).where(MetalPrice.macd >=0)               # Example
    ]

    tasks = []
    for idx, query in enumerate(queries):
        tasks.append(read_data(query))

    # Concurrently execute all tasks
    results = await asyncio.gather(*tasks)
    return results

# Main function to run concurrent reads
async def main_read():
    results = await concurrent_reads()    
    # pp.pprint(results)
    # Process results as needed    
    for idx, data in enumerate(results):
        # pp.pprint(rows)
        print(f"Results for query id = {idx}:")
        for row in data:
            print(f"ID: {row[0].id}, Metal: {row[0].metal}, Date: {row[0].date}, Price: {row[0].price}, MACD: {row[0].macd}, MACD_signal: {row[0].macd_signal}, RSI: {row[0].rsi}")            
            print()

# Run the main function using asyncio.run()
asyncio.run(main_read())