import asyncio
import pandas as pd
from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime
import logging
from functools import wraps
from typing import Tuple

# Configure logging
logging.basicConfig(filename='sql_inserts_Q5.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Define SQLAlchemy engine, specifying additional aiosqlite driver 
engine = create_async_engine('sqlite+aiosqlite:///metal_commodity_Q5.db')  # Use future=True for asyncio compatibility

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
def calculate_macd(prices: pd.Series, slow_period: int = 26, fast_period: int = 12, signal_period: int = 9) -> Tuple[pd.Series, pd.Series]:
    slow_ema = prices.ewm(span=slow_period).mean()
    fast_ema = prices.ewm(span=fast_period).mean()
    macd_line = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal_period).mean()
    return macd_line, signal_line

# Function to calculate RSI for a series of prices
def calculate_rsi(prices: pd.Series, window: int = 14) -> pd.Series:
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd_rsi(csv_file: str) -> Tuple[pd.DataFrame, pd.Index]:
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

# Define MetalPriceService class
class MetalPriceService:
    def __init__(self):
        self.async_session  = async_sessionmaker(bind=engine, expire_on_commit=False)

    # Define function to populate SQL database
    @log_sql
    async def populate_sql_table(self, df: pd.DataFrame, metals: pd.Index) -> None:
        # Create session
        async_session = self.async_session
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
    
    # Example: Define function to read data in SQL table
    @log_sql
    async def read_sql_table(self) -> None:
        pass
    
    # Example: Define function to delete data in SQL table
    @log_sql
    async def update_sql_table(self) -> None:
        pass
    
    # Example: Define function to delete data in SQL table
    @log_sql
    async def delete_sql_table(self) -> None:
        pass
    
# Define main function to run async tasks
async def main():    
    # Read CSV file and calculate MACD, RSI
    csv_file = 'MarketData_filtered.csv'
    service = MetalPriceService()
    
    # Populate SQL table
    df, metals =  calculate_macd_rsi(csv_file)        
    await service.populate_sql_table(df, metals)
    
    # Update SQL table
    await service.update_sql_table()
    
    # Read SQL table
    await service.read_sql_table()
    
    # Delete rows from SQL table
    await service.delete_sql_table()

# Execute the asyncio event loop
asyncio.run(main())