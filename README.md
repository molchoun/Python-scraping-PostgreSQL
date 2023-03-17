# Web Scraping with Beautiful Soup and PostgreSQL 

## Real Estate data scraping from [List.am](https://list.am)

## Description

This Python project demonstrates how to extract data from a website using the Beautiful Soup library, clean it using Pandas, and load it into a PostgreSQL database.
This is a Python web scraping project that uses Beautiful Soup library to extract data from a website. The scraped data is then cleaned using pandas and loaded into a PostgreSQL database.

The project is designed to scrape data from [insert website URL here], and extracts information about [insert information to be extracted here]. The scraped data is then cleaned and transformed using pandas to ensure that it is in a format that is suitable for loading into a database.

The data is loaded into a PostgreSQL database using [insert library or tool used for loading data here], making it easy to store and analyze the scraped data. The database schema is [insert schema design here], and can be easily modified to suit your specific needs.

This project is ideal for anyone who wants to learn about web scraping using Python, as well as how to work with pandas and PostgreSQL. The code is well-documented and easy to follow, and can be easily modified to work with other websites and data sources.

### Requirements

beautifulsoup4>
pandas
psycopg2-binary
numpy
SQLAlchemy

### Installation

To install the necessary dependencies for this project, run the following command:

```console
foo@bar:~$ pip install -r requirements.txt
```

### Usage
To use this project, follow these steps:

1. ne this repository to your local machine.
2. tall the required Python packages using pip: pip install -r requirements.txt.
3. ate a PostgreSQL database and set up the database credentials in config.py.
4. scrape.py to scrape data from the website, clean it using pandas, and load it into the PostgreSQL database

