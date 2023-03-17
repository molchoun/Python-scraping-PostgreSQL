# Web Scraping with Beautiful Soup and PostgreSQL 

## Real Estate data scraping from 

## Description

This Python project demonstrates how to extract data using the Beautiful Soup library, clean it using Pandas, and load it into a PostgreSQL database.
The project is designed to scrape a real estate data from an online marketplace website [List.am](https://list.am), and extract corresponding information about apartments, houses, commercial properties etc. The scraped data is then cleaned and transformed using pandas to ensure that it is in a format that is suitable for loading into a database.

The data is loaded into a PostgreSQL database mainly using psycopg2 - PostgreSQL database adapter, making it easy to store and analyze the scraped data. The database schema can be easily modified to suit your specific needs.

This project is ideal for anyone who wants to learn about web scraping using Python, as well as how to work with pandas and PostgreSQL. The code is well-documented and easy to follow.

### Requirements

* BeautifulSoup
* pandas
* psycopg2
* NumPy
* SQLAlchemy

### Installation

To install the necessary dependencies for this project, run the following command:

```console
foo@bar:~$ pip install -r requirements.txt
```

### Usage
To use this project, follow these steps:

1. Clone this repository to your local machine.
2. Install the required Python packages using pip: `pip install -r requirements.txt`.
3. Create a PostgreSQL database and set up the database credentials in `database.ini` file.
4. scrape.py to scrape data from the website, clean it using pandas, and load it into the PostgreSQL database

