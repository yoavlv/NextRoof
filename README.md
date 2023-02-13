# RealEstateTLV

Welcome to the Real Estate TLV project! This project is designed to help people find real estate properties in Tel Aviv, Israel. Whether you're looking for a place to live or you're an investor searching for the next big thing, this project has something for everyone.

Features
Search for properties by location, price, size, and more
Get detailed information about each property, including photos and descriptions
Save your favorite properties for later
Contact the property owner or agent directly from the app
Stay up-to-date with the latest real estate trends in Tel Aviv

The project is based on information from several sources:

www.govmap.gov.il - a government website that gives access to all real estate transactions from 1998 to the present in Shiral and in particular in Tel Aviv

www.nadlan.gov.il - another government site that provides information on real estate transactions in Israel and in Tel Aviv in particular - the information on this site is intended to complete missing information from the govmap site and collect additional providers that are not shown on the govmap site.

www.yad2.co.il - an Israeli site where private individuals, ranges, and real estate companies advertise their apartments for sale.

The project was built in several stages:
1. Collecting the information from the three sites in full - the information is collected through crawling and using an external API.
We divided the collected information into 3 tables: nadlan, govmap and yad2.

2. Clearing the information -
govmap, nadlan - we cleaned information irrelevant to our project such as real estate transactions of: parking lots, shops, land, etc...
and information that is not reliable in terms of the transaction price and the size of the property.
yad2 - We cleaned information that did not include price, unreliable price, and transactions of companies (we left only transactions of private individuals and brokers).

3. Consolidation of the information - after cleaning the information, we unified the information of govmap and nadlan so that we received more data on each transaction.

4. Researching the information - we used information visualization to research the information, learn about it and its reliability and quality.

5. After investigating various algorithms we came to the decision to use the RandomForestRegressor model which brought us reliable results.
After studying the model and checking it on the consolidated information of govmap and nadlan (past transactions)

6. Running the existing model on new transactions in the market in order to try and locate the most profitable transactions in the market today.


Requirements
To run this project, you will need:
Python 3.7 or higher
Jupyter Notebook
Pandas, Numpy, Matplotlib, and Seaborn libraries

Installation
The Real Estate TLV project is a web-based application and can be accessed directly from your browser. No downloads or installations are required.

Usage
To start using Real Estate TLV, simply go to the website and begin your search. You can filter your results by location, price, size, and other criteria to find the perfect property for you.

Contribution
We welcome contributions to the Real Estate TLV project! If you have an idea for a new feature, or if you've found a bug that needs to be fixed, please open a new issue in the GitHub repository. We appreciate your help
