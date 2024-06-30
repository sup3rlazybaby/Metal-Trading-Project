# Metal trading demontration

## Introduction

This project aims to demonstrate proficiency with python, SQL for data science project. The tasks involve data cleaning, managing data in SQL server, controlling the interaction with SQL server via asynchronous operation to perform calculations. The example is for calculating MACD and RSI for metal prices. These are important indicators to determine entry and exit strategy when trading.

This demonstration is broken down into multiple tasks to solved, each of which is encapsulated as a question. This problem-solving oriented approach enables better demonsration of the combination of skills necessary to solve each problem.

## Requirements

- Python 3.11.X
- Jupyter Notebook
- Pandas
- SQLAlchemy or any other SQL toolkit

---

## Questions

### Question 1: Python Basics and Data Manipulation
**Objective:** Demonstrate basic Python skills and data manipulation using Pandas.

**Task:**
- Load a given CSV file containing metal prices into a Pandas DataFrame.
- Filter the data to include only 'Copper' and 'Zinc' for the year 2021.
- Calculate the average price per month for each metal and plot it.

---

### Question 2: CRUD Operations in SQL Server
**Objective:** Basic SQL Server interactions.

**Task:**
- Create an SQL table schema to store time-series metal prices. Include fields like `Date`, `Metal`, `Price`.
- Demonstrate basic CRUD operations

---

### Question 3: Data Pipeline and Transformation
**Objective:** Show understanding of creating data pipelines and transformations.

**Task:**
- Using the CSV file from Question 1, filter the data to include only 'Copper' and 'Zinc' for the year 2020 & 2021.
- Calculate MACD (slow/medium/fast) and RSI for each metal historically.
- Use SQL inserts to populate the SQL table created in Question 2 with this generated data.

- Demonstrate the use of a decorator to log the execution of the SQL inserts.

---

### Question 4: Async Data Pipeline
- Modify Question 3 to write data to the database **asynchronously** .
- Read from the database 5 times *concurrantly* using **async** (hint: `asyncio.gather()`)

---
### Question 5: Code Maintainability
**Objective:** Gauge understanding of maintainable code architecture.

**Task:**
- Take one of your previously written code blocks and refactor it to be more maintainable and modular. Explain your decisions.


