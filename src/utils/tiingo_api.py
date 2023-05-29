from types import SimpleNamespace
from dataclasses import dataclass
import requests
import os

# creating the connection to the tiingo website
BASE_URL = "https://api.tiingo.com/tiingo"
API_TOKEN = os.getenv('TIINGO_API_TOKEN')

# first dataclass below - fundamentals
@dataclass
class DailyMultipliersData:
    date: str
    marketCap: float
    enterpriseVal: float
    peRatio: float
    pbRatio: float
    trailingPEG1Y: float


@dataclass
class IncomeStatement:
    dataCode: str
    value: float


@dataclass
class Overview:
    dataCode: str
    value: float


@dataclass
class FreeCashFlow:
    dataCode: str
    value: float


@dataclass
class BalanceSheet:
    dataCode: str
    value: float


@dataclass
class StatementData:
    incomeStatement: object
    balanceSheet: list[BalanceSheet]
    cashFlow: list[FreeCashFlow]
    incomeStatement: list[IncomeStatement]
    overview: list[Overview]

    def __getitem__(self, key):
        return self.balanceSheet[key]


@dataclass
class Fundamental:
    date: str
    year: int
    quarter: int
    statementData: StatementData

    def __getitem__(self, key):
        return self.statementData[key]


# second dataclass below - end of day prices
@dataclass
class EndOfDayPrices:
    date: str
    close: float
    high: float
    low: float
    open: float
    volume: float
    adjClose: float
    adjHigh: float
    adjLow: float
    adjOpen: float
    adjVolume: float
    divCash: float
    splitFactor: float


class TiingoApi:
    def __init__(self) -> None:
        super().__init__()

        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': API_TOKEN
        }

    # this functions returns the complete fundamental data in json format from tiingo per single stock
    def get_all_daily_fundamentals_data(self, ticker: str) -> list[Fundamental]:
        ticker = ticker.replace('.', '')
        url = f"{BASE_URL}/fundamentals/{ticker}/statements?token={API_TOKEN}"
        response = requests.get(url, headers=self.headers)
        return response.json(object_hook=lambda d: SimpleNamespace(**d))

    # this functions returns the complete end of day prices data in json format from tiingo per single stock
    def get_end_of_day_prices_by_date(self, ticker: str, start_date_str='2012-1-1') -> list[EndOfDayPrices]:
        ticker = ticker.replace('.', '')
        url = f"{BASE_URL}/daily/{ticker}/prices?startDate={start_date_str}&token={API_TOKEN}"
        response = requests.get(url, headers=self.headers)
        return response.json(object_hook=lambda d: SimpleNamespace(**d))

    # this functions returns the final daily multipliers result data in json format from tiingo per single stock
    def get_daily_multipliers(self, ticker: str) -> list[DailyMultipliersData]:
        ticker = ticker.replace('.', '')
        url = f"{BASE_URL}/fundamentals/{ticker}/daily?token={API_TOKEN}"
        response = requests.get(url, headers=self.headers)
        return response.json(object_hook=lambda d: SimpleNamespace(**d))

    # this functions returns the last update date in tiingo per single stock
    def get_last_update_date_daily(self, ticker: str) -> str | None:
        ticker = ticker.replace('.', '')
        url = f"{BASE_URL}/daily/{ticker}?token={API_TOKEN}"
        response = requests.get(url, headers=self.headers).json()
        if response is None or len(response) == 0:
            return None
        else:
            return response['endDate']

    # To request historical statement data limited by date range, use this endpoint
    def get_last_update_quarterly_fundamentals_date(self, ticker: str, start_date_str='2022-01-01') -> str | None:
        ticker = ticker.replace('.', '')
        url = f"{BASE_URL}/fundamentals/{ticker}/statements?startDate={start_date_str}&token={API_TOKEN}"
        response = requests.get(url, headers=self.headers).json()

        if response is None or len(response) == 0:
            return None
        else:
            return response[0]['date']

    def get_last_update_quarterly_fundamentals(self, ticker: str, start_date_str='2022-01-01') -> list[Fundamental]:
        ticker = ticker.replace('.', '')
        url = f"{BASE_URL}/fundamentals/{ticker}/statements?startDate={start_date_str}&token={API_TOKEN}"
        response = requests.get(url, headers=self.headers)
        return response.json(object_hook=lambda d: SimpleNamespace(**d))
