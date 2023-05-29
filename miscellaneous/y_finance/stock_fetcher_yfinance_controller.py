from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import logging

SNP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SNP500_WIKI_SYMBOL_COLUMN_NAME = 'Symbol'
DATE_FORMAT = '%Y-%m-%d'

# class StockFetcher below works when using the yfinance library
class StockFetcher:

    def __init__(self) -> None:
        super().__init__()
        # fetching the relevant table, containing the s&p500 tickers, from wikipedia
        datatable_snp500 = pd.read_html(SNP500_WIKI_URL)[0]

        # selecting only symbols' columns, converting them into a list
        self.snp500_tickers_lst = datatable_snp500[SNP500_WIKI_SYMBOL_COLUMN_NAME].tolist()

    # function that retrieves the data according to a given stock ticker (from user)
    def get_stock_data(self, stock_ticker: str, start_date_str: str, num_of_days=1) -> pd.DataFrame:
        if stock_ticker not in self.snp500_tickers_lst:
            raise Exception(f"Stock ticker symbol {stock_ticker} is not in S&P500")

        # get end_date from start_date and num_of_days
        end_date = datetime.strptime(start_date_str, DATE_FORMAT) + timedelta(days=num_of_days)
        end_date_str = datetime.strftime(end_date, DATE_FORMAT)
        logging.info(f"Fetching stock data for {stock_ticker} between {start_date_str} to {end_date_str}")
        yf_data = yf.download(stock_ticker, start=start_date_str, end=end_date_str).reset_index()
        logging.debug(yf_data)
        return yf_data

    def get_balance_sheet(self, ticker: yf.Ticker) -> pd.DataFrame:
        # using yfinance library
        stock_balance_sheet = ticker.quarterly_balancesheet
        return stock_balance_sheet

    def get_historical_data(self, ticker: yf.Ticker) -> pd.DataFrame:
        # using yfinance library
        stock_historical_data = ticker.history(period='max')
        return stock_historical_data

    def get_cash_flow(self, ticker: yf.Ticker) -> pd.DataFrame:
        # using yfinance library
        stock_cash_flow = ticker.cashflow
        return stock_cash_flow

    def get_income_statement(self, ticker: yf.Ticker) -> pd.DataFrame:
        # using yfinance library
        stock_income_statement = ticker.financials
        return stock_income_statement
