#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import the outside folder package
import os
import sys
import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# import the package

from cryptocmdsupply import CmcSupplyScraper

for i in range(365):
    target_date = "01-01-2021"
    target_date = datetime.datetime.strptime(target_date, "%d-%m-%Y")
    target_date += datetime.timedelta(days=i)
    target_date = target_date.strftime("%d-%m-%Y")
    print(target_date)
    # initialize the scraper without limit and fiat
    scraper = CmcSupplyScraper(target_date=target_date, limit = 3000, fiat = "USD")
    # data to csv
    scraper.export("csv", name="../../Web3/data/supply/cmc_supply_" + target_date)



