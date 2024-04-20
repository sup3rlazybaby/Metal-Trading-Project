import asyncio
import pandas as pd
from sqlalchemy import select, update, Column, Integer, String, Float, Date
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from datetime import datetime
import logging
from functools import wraps

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

# Define MetalPriceService class
class MetalPriceService:
    def __init__(self):
        self.async_session  = async_sessionmaker(bind=engine, expire_on_commit=False)

    async def populate_from_csv(self, filename: str):
        async_session = self.async_session()
        df = pd.read_csv(filename)

        async with async_session as session:
            for index, row in df.iterrows():
                date = row['Dates']
                for metal, price in row.items():
                    if metal != 'Dates':  # Skip the 'Dates' column
                        await session.execute(MetalPrice.__table__.insert().values(
                            date=date, metal=metal, price=price))
                await session.commit()
            
    async def calculate_macd_rsi(self):
        async_session = self.async_session()
        
        async with self.async_session() as session:
            metal_prices = await session.execute(select(MetalPrice))
            for metal_price in metal_prices:
                metal = metal_price.metal
                # price = metal_price.price

                prices = [row.price for row in metal_prices if row.metal == metal]
                prices = pd.Series(prices)
                macd_line, macd_signal = calculate_macd(prices)
                rsi = calculate_rsi(prices)

                await session.execute(update(MetalPrice).where(
                    MetalPrice.metal == metal).values(macd=macd_line, macd_signal=macd_signal, rsi=rsi))
            await session.commit()

            


# Define main function to run async tasks
@log_sql
async def main(filename: str):
    service = MetalPriceService()
    await service.populate_from_csv(filename)
    await service.calculate_macd_rsi()

# Execute the asyncio event loop
asyncio.run(main('MarketData_filtered.csv'))
