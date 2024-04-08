# Take-Home Assessment for Intern - Oil Desk

## Introduction

This take-home assessment is designed to gauge your proficiency in Python and programming, as these are key aspects of OilDesk Intern role. The assessment consists of 4 questions, each increasing in complexity. Aim to answer as many as you can, and ensure that your code is clean, well-commented, and accompanied by explanations of your approach and reasoning.

## Requirements

- Python 3.11.X
- Jupyter Notebook
- Pandas
- SQLAlchemy or any other SQL toolkit

## Instructions

1. Fork this repository to your GitHub account.
2. Clone the forked repository and create a `solutions/` directory.
3. Answer the questions listed below in a Jupyter Notebook under the `solutions/` directory.
4. Ensure that all code is clean, well-commented, and accompanied by explanations of your approach and reasoning.
5. Each question should be answered in a separate Jupyter Notebook.
6. Any packages you use must be added to the `requirements.txt` file 
7. Push the completed Notebook back to your GitHub repository.
8. Share the GitHub link with us by the deadline provided.

## Additional Notes

- You are free to use any Python libraries you wish, the requirements above are purely a suggestion.
- You may find it useful to create an SQLITE database to store the data for the SQL questions.

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



## Submission Guidelines

1. Ensure that all code is clean, well-commented, and accompanied by explanations of your approach and reasoning.
2. Commit your Jupyter Notebook and any auxiliary files to your forked GitHub repository.
3. Submit the GitHub repository URL by the deadline.

---

Good luck!
