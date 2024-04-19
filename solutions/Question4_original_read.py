import pandas as pd
import pprint as pp
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
engine = create_async_engine('sqlite+aiosqlite:///metal_commodity_Q4.db')  # Use future=True for asyncio compatibility

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
        select(MetalPrice).where(MetalPrice.date == '2021-01-01'),      # Should return 2 database rows
        select(MetalPrice).where(MetalPrice.id == 1),                   # Should return 1 database row
        select(MetalPrice).where(MetalPrice.metal == 'TIN'),            # Should return 0 database row
        select(MetalPrice).where(MetalPrice.id == 76),                  # Should return 1 database row
        select(MetalPrice).where(MetalPrice.id == 16),                  # Should return 1 database row
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