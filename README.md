# Python ETL Pipeline

## 📌 Project Overview
This project simulates a real-world ETL scenario.  
It extracts data on the top 10 largest banks by market capitalization, transforms the values into multiple currencies (USD, GBP, EUR, INR), and loads the data into both a CSV file and an SQL database for further analysis by international teams.

---

## 🎯 Project Objectives
- Simulate a real-world ETL pipeline using Python  
- Extract financial data on the top 10 global banks from a public website  
- Transform market capitalization values into multiple currencies (GBP, EUR, INR) using provided exchange rates  
- Load the transformed data into both a CSV file and an SQLite database  
- Run SQL queries based on office location requirements  
- Demonstrate clean, modular, and maintainable data engineering code  

---

## 🛠️ Technologies Used
- Python 3.8+
- pandas
- requests
- BeautifulSoup
- sqlite3
- SQL
- logging

---

## 📁 Project Structure

```
Python-ETL-Pipeline/
├── banks_project.py         # Main script that handles the full ETL process
├── exchange_rate.csv        # Exchange rate data used for currency conversion
├── Largest_banks_data.csv   # Extracted data saved as CSV
├── Banks.db                 # SQLite database containing the final table
├── code_log.txt             # Log file tracking process execution
└── README.md                # Project documentation
```

---

## ▶️ Running the ETL Pipeline

To execute the complete ETL process, run:

```bash
python banks_project.py
```

The pipeline will:
- Extract raw bank data from the web
- Transform values into GBP, EUR, and INR using exchange rates
- Load the data into a CSV file and an SQLite database
- Log each step with timestamps for traceability

---

## 📚 Source

This project was completed as part of the  
**Python Project for Data Engineering – IBM (Coursera)**  
Link: [https://www.coursera.org/learn/python-project-for-data-engineering](https://www.coursera.org/learn/python-project-for-data-engineering)


```
