#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'Rohit Gupta'

"""
Cryptocurrency price History from coinmarketcap.com
"""

from __future__ import print_function

__all__ = ["CmcSupplyScraper"]

import os
import csv
import tablib
import numpy as np
import warnings
from datetime import datetime
from .utils import download_coin_data, InvalidParameters


class CmcSupplyScraper(object):
    """
    Scrape cryptocurrency historical market price data from coinmarketcap.com

    """

    def __init__(
        self,
        target_date = None,
        limit = 100,
        fiat="USD"
    ):
        """
        :param target_date: (optional) date for which data is to be scraped.
        :param limit: (optional) limit of the data to be scraped.
        :param order_ascending: data ordered by 'Date' in ascending order (i.e. oldest first).

        """

        self.target_date = target_date
        self.limit = limit
        self.fiat = fiat
        # {"id":1,"name":"Bitcoin","symbol":"BTC","slug":"bitcoin","date_added":"2010-07-13T00:00:00.000Z","tags":["mineable","pow","sha-256","store-of-value","state-channel","coinbase-ventures-portfolio","three-arrows-capital-portfolio","polychain-capital-portfolio","binance-labs-portfolio","blockchain-capital-portfolio","boostvc-portfolio","cms-holdings-portfolio","dcg-portfolio","dragonfly-capital-portfolio","electric-capital-portfolio","fabric-ventures-portfolio","framework-ventures-portfolio","galaxy-digital-portfolio","huobi-capital-portfolio","alameda-research-portfolio","a16z-portfolio","1confirmation-portfolio","winklevoss-capital-portfolio","usv-portfolio","placeholder-ventures-portfolio","pantera-capital-portfolio","multicoin-capital-portfolio","paradigm-portfolio","bitcoin-ecosystem","ftx-bankruptcy-estate"],"max_supply":21000000,"circulating_supply":17556425,"total_supply":17556425,"infinite_supply":false,"platform":null,"cmc_rank":1,"self_reported_circulating_supply":null,"self_reported_market_cap":null,"tvl_ratio":null,"last_updated":"2024-02-22T08:59:57.167Z","quote":{"USD":{"price":3810.42743065,"volume_24h":10794227451.2229,"percent_change_1h":-0.341595,"percent_change_24h":-8.23861,"percent_change_7d":3.64049,"market_cap":66897483404.14943,"last_updated":"2024-02-22T08:59:57.167Z"}}}

        self.headers = ["id", "name", "symbol", "slug", "date_added", "tags", "max_supply", "circulating_supply", "total_supply", "infinite_supply", "platform", "cmc_rank", "self_reported_circulating_supply", "self_reported_market_cap", "tvl_ratio", "last_updated", "quote"]
        self.rows = [] 

        if not (self.target_date):
            raise InvalidParameters(
                "Please provide 'target_date' for which data is to be scraped."
            )

    def __repr__(self):
        return (
            "<CmcScraper(target_date={0}, limit={1}, fiat={2})>".format(
                self.target_date, self.limit, self.fiat
            )
        )

    def _download_data(self, **kwargs):
        """
        This method downloads the data.
        :param forced: (optional) if ``True``, data will be re-downloaded.
        :return:
        """

        forced = kwargs.get("forced")

        if self.rows and not forced:
            return

        coin_data = download_coin_data(
            self.target_date, self.limit, self.fiat
        )

        for _row in coin_data["data"]:
            
            date = datetime.strptime(
                self.target_date, "%d-%m-%Y"
            ).strftime("%d-%m-%Y")
            
            
            # check every field
            for field in self.headers:
                if field not in _row:
                    _row[field] = np.nan


            row = [
                date,
                _row["id"],
                _row["name"],
                _row["symbol"],
                _row["slug"],
                _row["date_added"],
                _row["tags"],
                _row["max_supply"],
                _row["circulating_supply"],
                _row["total_supply"],
                _row["infinite_supply"],
                _row["cmc_rank"],
                _row["self_reported_circulating_supply"],
                _row["self_reported_market_cap"],
                _row["tvl_ratio"],
                _row["last_updated"],
                _row["quote"]
            ]

            self.rows.insert(0, row)

        self.end_date, self.target_date = self.rows[0][0], self.rows[-1][0]

    def get_data(self, format = "", verbose=False, **kwargs):
        """
        This method returns the downloaded data in specified format.
        :param format: extension name of data format. Available: json, xls, yaml, csv, dbf, tsv, html, latex, xlsx, ods
        :param verbose: (optional) Flag to enable verbose only.
        :param kwargs: Optional arguments that data downloader takes.
        :return:
        """

        self._download_data(**kwargs)
        if verbose:
            print(*self.headers, sep=", ")

            for row in self.rows:
                print(*row, sep=", ")
        elif format:
            data = tablib.Dataset()
            data.headers = self.headers
            for row in self.rows:
                data.append(row)
            return data.export(format)
        else:
            return self.headers, self.rows

    def get_dataframe(self, date_as_index=False, **kwargs):
        """
        This gives scraped data as DataFrame.
        :param date_as_index: make 'Date' as index and remove 'Date' column.
        :param kwargs: Optional arguments that data downloader takes.
        :return: DataFrame of the downloaded data.
        """

        try:
            import pandas as pd
        except ImportError:
            pd = None

        if pd is None:
            raise NotImplementedError(
                "DataFrame Format requires 'pandas' to be installed."
                "Try : pip install pandas"
            )

        self._download_data(**kwargs)

        dataframe = pd.DataFrame(data=self.rows, columns=self.headers)

        # convert 'Date' column to datetime type
        dataframe["Date"] = pd.to_datetime(
            dataframe["Date"], format="%d-%m-%Y", dayfirst=True
        )

        if date_as_index:
            # set 'Date' column as index and drop the the 'Date' column.
            dataframe.set_index("Date", inplace=True)

        return dataframe

    def export_csv(self, csv_name=None, csv_path=None, **kwargs):
        """
        This exports scraped data into a csv.
        :param csv_name: (optional) name of csv file.
        :param csv_path: (optional) path to where export csv file.
        :param kwargs: Optional arguments that data downloader takes.
        :return:
        """
        warnings.warn(
            "export_csv will be deprecated; Use 'export' method instead, e.g. export('csv')",
            PendingDeprecationWarning,
            stacklevel=2,
        )

        self._download_data(**kwargs)

        if csv_path is None:
            # Export in current directory if path not specified
            csv_path = os.getcwd()

        if csv_name is None:
            # Make name fo file in format of {coin_code}_{fiat}_{target_date}_{end_date}.csv
            csv_name = "{0}_{1}_{2}_{3}.csv".format(
                self.coin_code, self.fiat, self.target_date, self.end_date
            )

        if not csv_name.endswith(".csv"):
            csv_name += ".csv"

        _csv = "{0}/{1}".format(csv_path, csv_name)

        try:
            with open(_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(
                    csvfile, delimiter=",", quoting=csv.QUOTE_NONNUMERIC
                )
                writer.writerow(self.headers)
                for data in self.rows:
                    writer.writerow(data)
        except IOError as err:
            errno, strerror = err.args
            print("I/O error({0}): {1}".format(errno, strerror))

    def export(self, format, name=None, path=None, **kwargs):
        """
        Exports the data to specified file format
        :param format: extension name of file format. Available: json, xls, yaml, csv, dbf, tsv, html, latex, xlsx, ods
        :param name: (optional) name of file.
        :param path: (optional) output file path.
        :param kwargs: Optional arguments that data downloader takes.
        :return:
        """

        data = self.get_data(format, **kwargs)

        if path is None:
            # Export in current directory if path not specified
            path = os.getcwd()

        if name is None:
            # Make name of file in format: {coin_code}_{fiat}_{target_date}_{end_date}.csv
            name = "{0}_{1}-{2}_{3}".format(
                self.coin_code, self.fiat, self.target_date, self.end_date
            )

        if not name.endswith(".{}".format(format)):
            name += ".{}".format(format)

        _file = "{0}/{1}".format(path, name)

        try:
            with open(_file, "wb") as f:
                if type(data) is str:
                    f.write(data.encode("utf-8"))
                else:
                    f.write(data)
        except IOError as err:
            errno, strerror = err.args
            print("I/O error({0}): {1}".format(errno, strerror))
        except Exception as err:
            print("format: {0}, Error: {1}".format(format, err))
