### CSE6242 Final Project, Fall 2022
### Social Media Sentiment Analysis

## Developed by Malcolm Almuntazar-Harris, Alek Festekjian, Kevin Caley Ashley Roakes, Farrah Elfayoumy, Dennise Vieyra

## Description
Our project reviews retail investor sentiment on social media and forecast stocks from the time period of 2016-2022. This tool aggregates the overall user sentiment across a number of stocks and can be translated into a predicted price and investment advisement

## Installation/Access

The project can be accessed using the following link:

https://cse6242-finalproject.herokuapp.com/

To run the ML model on price predictions you need to set up your environment by running these download 

pip install jupyterlab
pip install bumpy
pip install pandas
pip install prophet

Then open stock_new_analysis.ipynb using jupyterlab by running the command

jupyter notebook

or by uploading the notebook to Paperspace and then run each cell.

## Installation - Further Development Work

Further progress if it was made open source could be performed by creating a virtual environment (e.g. ‘sentimentanalysis’) and installing the dependencies outlined in the requirements.txt file. Once installed, one can create a batch file (image below) in that directory and point it to the main.py file in the cloned repository. All required python, javascript, html and css files are in the app folder.

Call ./sentimentanalysis\Scripts\activate.bat

Set FLASK_APP=C:\Users\[YOURUSERNAME]\Document\Github\CSE6242\main
Set FLASK_RUN_PORT=8000
Set FLASK_DEBUG=1
flask run



## Use

After accessing the project, users can use the tool to examine historical prices, predicted prices, and user sentiment to buy/sell/hold the stock.

First, select a begin date, end date and the stock you want to examine. Then select the button labeled “Stock Prediction” to repopulate the project elements with your selected parameters.

The chart in the center will repopulate to show the timespan of your choosing along with your chosen stock, the S&P 500, the Dow Jones and the machine learning model’s predicted price. 

The top right of the website includes a table which includes more information for an investor including the predicted stock price of next 7 days and market beta relative to the S&P 500 and the Dow Jones.

If an investor wants to use sentiment to make their decisions, the project displays the market sentiment based on Reddit data. The bottom right chart displays the amount of comments that promote buying, selling or holding the stock. Examples of these comments display on a rolling basis at the top of the project.


## Backend 

Python (Flask), SQL (Postgres)

Price prediction used a Prophet model written in a Jupyter notebook

Sentiment Analysis was done using VADER (Valence Aware Dictionary for Sentiment Reasoning) from the Natural Language Toolkit (nltk) in Python

## Frontend

HTML, CSS, Javascript (extends to Jquery and ChartJS libraries) and Jinja