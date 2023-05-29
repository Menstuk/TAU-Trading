from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
import sys
from pathlib import Path

src_dir = Path(__file__).parent.parent.absolute().__str__() # get parent of parent
print(src_dir)
sys.path.append(src_dir)

from dataclasses import dataclass
import logging
import numpy as np
import pandas as pd
import sqlalchemy as sqlalch
from sqlalchemy import func
from utils.database import get_db
from utils import models
# from utils import pfcf_ratio_calculation
from utils.tiingo_api import TiingoApi
from datetime import timedelta
from datetime import datetime


@dataclass
class YearAndQuarter:
    year: int
    quarter: int

class UpdateDB:
    t_api = TiingoApi()

    def get_table_latest_year_and_quarter(self, ticker: int, table: models) -> YearAndQuarter:
        db = get_db()
        latest_year_and_quarter_lst = sqlalch.select(table.year, table.quarter). \
            filter(table.stock_id == ticker).order_by(table.year.desc(), table.quarter.desc())
        latest_year_and_quarter_res = db.execute(latest_year_and_quarter_lst).first()

        return latest_year_and_quarter_res

    def create_ticker_to_id_dictionary_from_db(self) -> dict[str, int]:
        db = get_db()

        results = db.query(models.StocksByID).all()
        dict1 = {}
        for stock_by_id_item in results:
            dict1[stock_by_id_item.stock_name] = stock_by_id_item.id

        return dict1

    def update_end_of_day_prices_table(self) -> None:
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # get last date in our database
            last_date_in_table = sqlalch.select([func.max(models.EndOfDayPrices.date)]). \
                filter(models.EndOfDayPrices.stock_id == ticker_id)
            last_date_in_table_res = db.execute(last_date_in_table).first()[0]

            # compare between our last_date_in_table and tiingo_last_date + action for "not the same date" or "the same date"
            last_tiingo_update_date = self.t_api.get_last_update_date_daily(ticker_name)

            if last_tiingo_update_date:

                if last_tiingo_update_date > last_date_in_table_res:
                    # adding 1 day to our db last updated date
                    increment_start_date = (
                                datetime.strptime(last_date_in_table_res, "%Y-%m-%d") + timedelta(days=1)).date()
                    end_of_day_list = self.t_api.get_end_of_day_prices_by_date(ticker_name,
                                                                               start_date_str=increment_start_date)
                    # for is used just in case more than 1 date needs to be updated
                    for end_of_day_item in end_of_day_list:
                        model = models.EndOfDayPrices(stock_id=ticker_id, date=end_of_day_item.date[:10],
                                                      close_price=end_of_day_item.close)
                        db.add(model)
                    db.commit()
                    print(f"end of day for {ticker_name} updated successfully")

                else:
                    print(f"Nothing has changed with {ticker_name}")

            else:
                print(f"No new data was found for {ticker_name}")

    def update_balance_sheet_table(self) -> None:
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest date existing in our db "balance sheet" table per stock
            latest_db_date = sqlalch.select([func.max(models.QuarterlyBalanceSheetData.date)]). \
                filter(models.QuarterlyBalanceSheetData.stock_id == ticker_id)
            latest_db_date_res = db.execute(latest_db_date).first()[0]

            # compare between our last_date_in_table and tiingo_last_date + action for "not the same date" or "the same date"
            last_tiingo_update_date = self.t_api.get_last_update_quarterly_fundamentals_date(ticker_name)

            if last_tiingo_update_date:
                if last_tiingo_update_date > latest_db_date_res:
                    fundamentals_lst = self.t_api.get_last_update_quarterly_fundamentals(ticker_name)
                    for fundamental_item in fundamentals_lst:
                        if fundamental_item.date == last_tiingo_update_date:
                            # create a dictionary from array
                            dict1 = {}

                            # if there is no balance sheet then dictionary will be empty and rows with None values will be created
                            if hasattr(fundamental_item.statementData, 'balanceSheet'):
                                for key_value in fundamental_item.statementData.balanceSheet:
                                    dict1[key_value.dataCode] = key_value.value

                            new_obj = models.QuarterlyBalanceSheetData(
                                year=fundamental_item.year,
                                stock_id=ticker_id,
                                date=fundamental_item.date,
                                quarter=fundamental_item.quarter,
                                debtCurrent=dict1.get("debtCurrent"),
                                taxAssets=dict1.get("taxAssets"),
                                investmentsCurrent=dict1.get("investmentsCurrent"),
                                totalAssets=dict1.get("totalAssets"),
                                acctPay=dict1.get("acctPay"),
                                accoci=dict1.get("accoci"),
                                totalLiabilities=dict1.get("totalLiabilities"),
                                acctRec=dict1.get("acctRec"),
                                intangibles=dict1.get("intangibles"),
                                ppeq=dict1.get("ppeq"),
                                deferredRev=dict1.get("deferredRev"),
                                cashAndEq=dict1.get("cashAndEq"),
                                assetsNonCurrent=dict1.get("assetsNonCurrent"),
                                taxLiabilities=dict1.get("taxLiabilities"),
                                investments=dict1.get("investments"),
                                equity=dict1.get("equity"),
                                retainedEarnings=dict1.get("retainedEarnings"),
                                deposits=dict1.get("deposits"),
                                assetsCurrent=dict1.get("assetsCurrent"),
                                investmentsNonCurrent=dict1.get("investmentsNonCurrent"),
                                debt=dict1.get("debt"),
                                debtNonCurrent=dict1.get("debtNonCurrent"),
                                liabilitiesNonCurrent=dict1.get("liabilitiesNonCurrent"),
                                liabilitiesCurrent=dict1.get("liabilitiesCurrent"),
                                sharesBasic=dict1.get("sharesBasic")
                            )
                            db.add(new_obj)

                    db.commit()

                    print(f"quarterly balance sheet for {ticker_name} updated successfully")

                else:
                    print(f"No new quarterly balance sheet data was found for {ticker_name}")
            else:
                print(f"No new data was found for {ticker_name}")

    def update_cash_flow_table(self) -> None:
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest date existing in our db "cash flow" table per stock
            latest_db_date = sqlalch.select([func.max(models.QuarterlyCashFlow.date)]). \
                filter(models.QuarterlyCashFlow.stock_id == ticker_id)
            latest_db_date_res = db.execute(latest_db_date).first()[0]

            # compare between our last_date_in_table and tiingo_last_date + action for "not the same date" or "the same date"
            last_tiingo_update_date = self.t_api.get_last_update_quarterly_fundamentals_date(ticker_name)

            if last_tiingo_update_date:
                if last_tiingo_update_date > latest_db_date_res:
                    fundamentals_lst = self.t_api.get_last_update_quarterly_fundamentals(ticker_name)
                    for fundamental_item in fundamentals_lst:
                        if fundamental_item.date == last_tiingo_update_date:
                            # create a dictionary from array
                            dict1 = {}

                            # if there is no cash floe report then dictionary will be empty and rows with None values will be created
                            if hasattr(fundamental_item.statementData, 'cashFlow'):
                                for key_value in fundamental_item.statementData.cashFlow:
                                    dict1[key_value.dataCode] = key_value.value

                            new_obj = models.QuarterlyCashFlow(
                                year=fundamental_item.year,
                                stock_id=ticker_id,
                                date=fundamental_item.date,
                                quarter=fundamental_item.quarter,
                                ncfi=dict1.get("ncfi"),
                                capex=dict1.get("capex"),
                                ncfx=dict1.get("ncfx"),
                                ncff=dict1.get("ncff"),
                                sbcomp=dict1.get("sbcomp"),
                                ncf=dict1.get("ncf"),
                                payDiv=dict1.get("payDiv"),
                                businessAcqDisposals=dict1.get("businessAcqDisposals"),
                                issrepayDebt=dict1.get("issrepayDebt"),
                                issrepayEquity=dict1.get("issrepayEquity"),
                                investmentsAcqDisposals=dict1.get("investmentsAcqDisposals"),
                                freeCashFlow=dict1.get("freeCashFlow"),
                                ncfo=dict1.get("ncfo"),
                                depamor=dict1.get("depamor")
                            )
                            db.add(new_obj)

                    db.commit()

                    print(f"quarterly cash flow for {ticker_name} updated successfully")

                else:
                    print(f"No new quarterly cash flow data was found for {ticker_name}")
            else:
                print(f"No new data was found for {ticker_name}")

    def update_income_statement_table(self) -> None:
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest date existing in our db "income statement" table per stock
            latest_db_date = sqlalch.select([func.max(models.QuarterlyIncomeStatement.date)]). \
                filter(models.QuarterlyIncomeStatement.stock_id == ticker_id)
            latest_db_date_res = db.execute(latest_db_date).first()[0]

            # compare between our last_date_in_table and tiingo_last_date + action for "not the same date" or "the same date"
            last_tiingo_update_date = self.t_api.get_last_update_quarterly_fundamentals_date(ticker_name)

            if last_tiingo_update_date:
                if last_tiingo_update_date > latest_db_date_res:
                    fundamentals_lst = self.t_api.get_last_update_quarterly_fundamentals(ticker_name)
                    for fundamental_item in fundamentals_lst:
                        if fundamental_item.date == last_tiingo_update_date:
                            # create a dictionary from array
                            dict1 = {}

                            # if there is no income statement then dictionary will be empty and rows with None values will be created
                            if hasattr(fundamental_item.statementData, 'incomeStatement'):
                                for key_value in fundamental_item.statementData.incomeStatement:
                                    dict1[key_value.dataCode] = key_value.value

                                new_obj = models.QuarterlyIncomeStatement(
                                    year=fundamental_item.year,
                                    stock_id=ticker_id,
                                    date=fundamental_item.date,
                                    quarter=fundamental_item.quarter,
                                    ebit=dict1.get("ebit"),
                                    epsDil=dict1.get("epsDil"),
                                    rnd=dict1.get("rnd"),
                                    shareswa=dict1.get("shareswa"),
                                    taxExp=dict1.get("taxExp"),
                                    opinc=dict1.get("opinc"),
                                    costRev=dict1.get("costRev"),
                                    grossProfit=dict1.get("grossProfit"),
                                    ebitda=dict1.get("ebitda"),
                                    nonControllingInterests=dict1.get("nonControllingInterests"),
                                    netIncDiscOps=dict1.get("netIncDiscOps"),
                                    eps=dict1.get("eps"),
                                    intexp=dict1.get("intexp"),
                                    shareswaDil=dict1.get("shareswaDil"),
                                    revenue=dict1.get("revenue"),
                                    netinc=dict1.get("netinc"),
                                    opex=dict1.get("opex"),
                                    consolidatedIncome=dict1.get("consolidatedIncome"),
                                    netIncComStock=dict1.get("netIncComStock"),
                                    ebt=dict1.get("ebt"),
                                    prefDVDs=dict1.get("prefDVDs"),
                                    sga=dict1.get("sga"),
                                )
                            db.add(new_obj)

                    db.commit()

                    print(f"quarterly income statement for {ticker_name} updated successfully")

                else:
                    print(f"No new income statement sheet data was found for {ticker_name}")
            else:
                print(f"No new data was found for {ticker_name}")

    def update_overview_table(self) -> None:
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest date existing in our db "overview" table per stock
            latest_db_date = sqlalch.select([func.max(models.QuarterlyOverview.date)]). \
                filter(models.QuarterlyOverview.stock_id == ticker_id)
            latest_db_date_res = db.execute(latest_db_date).first()[0]

            # compare between our last_date_in_table and tiingo_last_date + action for "not the same date" or "the same date"
            last_tiingo_update_date = self.t_api.get_last_update_quarterly_fundamentals_date(ticker_name)

            if last_tiingo_update_date:
                if last_tiingo_update_date > latest_db_date_res:
                    fundamentals_lst = self.t_api.get_last_update_quarterly_fundamentals(ticker_name)
                    for fundamental_item in fundamentals_lst:
                        if fundamental_item.date == last_tiingo_update_date:
                            # create a dictionary from array
                            dict1 = {}

                            # if there is no overview report then dictionary will be empty and rows with None values will be created
                            if hasattr(fundamental_item.statementData, 'overview'):
                                for key_value in fundamental_item.statementData.overview:
                                    dict1[key_value.dataCode] = key_value.value

                            new_obj = models.QuarterlyOverview(
                                year=fundamental_item.year,
                                stock_id=ticker_id,
                                date=fundamental_item.date,
                                quarter=fundamental_item.quarter,
                                longTermDebtEquity=dict1.get("longTermDebtEquity"),
                                shareFactor=dict1.get("shareFactor"),
                                bookVal=dict1.get("bookVal"),
                                roa=dict1.get("roa"),
                                currentRatio=dict1.get("currentRatio"),
                                roe=dict1.get("roe"),
                                grossMargin=dict1.get("grossMargin"),
                                piotroskiFScore=dict1.get("piotroskiFScore"),
                                epsQoQ=dict1.get("epsQoQ"),
                                revenueQoQ=dict1.get("revenueQoQ"),
                                profitMargin=dict1.get("profitMargin"),
                                rps=dict1.get("rps"),
                                bvps=dict1.get("bvps")

                            )
                            db.add(new_obj)

                    db.commit()

                    print(f"quarterly overview for {ticker_name} updated successfully")

                else:
                    print(f"No new overview data was found for {ticker_name}")
            else:
                print(f"No new data was found for {ticker_name}")

    def update_full_daily_multipliers_table(self) -> None:

        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest date existing in our db "full daily multiplier" table per stock
            latest_db_date = sqlalch.select([func.max(models.FullDailyMultipliers.date)]). \
                filter(models.FullDailyMultipliers.stock_id == ticker_id)
            latest_db_date_res = db.execute(latest_db_date).first()[0]

            # compare between our last_date_in_table and tiingo_last_date + action for "not the same date" or "the same date"
            last_tiingo_update_date = self.t_api.get_last_update_date_daily(ticker_name)

            if last_tiingo_update_date:

                if last_tiingo_update_date > latest_db_date_res:
                    daily_multipliers_lst = self.t_api.get_daily_multipliers(ticker_name)

                    # for loop is used just in case more than 1 date needs to be updated
                    for daily_multipliers_item in daily_multipliers_lst:
                        model = models.FullDailyMultipliers(stock_id=ticker_id,
                                                            date=daily_multipliers_item.date[:10],
                                                            market_cap=daily_multipliers_item.marketCap,
                                                            enterprise_val=daily_multipliers_item.enterpriseVal,
                                                            pe_ratio=daily_multipliers_item.peRatio,
                                                            pb_ratio=daily_multipliers_item.pbRatio,
                                                            trailing_peg_1_y=daily_multipliers_item.trailingPEG1Y,
                                                            )
                        db.add(model)
                    db.commit()
                    print(f"daily multipliers for {ticker_name} updated successfully")

                else:
                    print(f"Nothing has changed with {ticker_name}")
            else:
                print(f"No new data was found for {ticker_name}")


    def update_graham_number_table(self) -> None:
        print(f"this is working")
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest year and quarter existing in our db "graham number" table per stock
            latest_year_and_quarter = self.get_table_latest_year_and_quarter(ticker_id, models.GrahamNumber)
            if latest_year_and_quarter:
                latest_current_year = latest_year_and_quarter.year
                latest_current_quarter = latest_year_and_quarter.quarter

                # extract values from "balance sheet", "overview" and "income statement" tables to check if all updated:
                latest_year_and_quarter_balance = self.get_table_latest_year_and_quarter(ticker_id,
                                                                                         models.QuarterlyBalanceSheetData)
                latest_year_and_quarter_overview = self.get_table_latest_year_and_quarter(ticker_id,
                                                                                          models.QuarterlyOverview)
                latest_year_and_quarter_income = self.get_table_latest_year_and_quarter(ticker_id,
                                                                                        models.QuarterlyIncomeStatement)

                latest_year_balance = latest_year_and_quarter_balance.year
                latest_quarter_balance = latest_year_and_quarter_balance.quarter
                latest_year_overview = latest_year_and_quarter_overview.year
                latest_quarter_overview = latest_year_and_quarter_overview.quarter
                latest_year_statement_income = latest_year_and_quarter_income.year
                latest_quarter_statement_income = latest_year_and_quarter_income.quarter

                # now check if latest year and quarter exist and match in all three tables:
                if latest_year_and_quarter_balance and latest_year_and_quarter_overview and latest_year_and_quarter_income:
                    if latest_year_balance == latest_year_overview and latest_year_balance == latest_year_statement_income:
                        if latest_quarter_balance == latest_quarter_overview and latest_quarter_balance == latest_quarter_statement_income:
                            if latest_current_year > latest_year_balance:  # graham number cannot be updated
                                print(f"No new data was found for {ticker_name}, graham number can't be updated")
                            else:
                                if (latest_current_quarter < latest_quarter_balance) and (latest_current_year == latest_year_balance):  # year has changed, graham number for sure needs to be updated

                                    # Retrieve the latest values necessary for graham number calculation:
                                    balance_sheet_sharesbasic = sqlalch.select(
                                        models.QuarterlyBalanceSheetData.sharesBasic). \
                                        filter(models.QuarterlyBalanceSheetData.stock_id == ticker_id,
                                               models.QuarterlyBalanceSheetData.year == latest_year_balance,
                                               models.QuarterlyBalanceSheetData.quarter == latest_quarter_balance)
                                    sharesbasic_res = db.execute(balance_sheet_sharesbasic).first()[0]

                                    overview_bookval = sqlalch.select(models.QuarterlyOverview.bookVal). \
                                        filter(models.QuarterlyOverview.stock_id == ticker_id,
                                               models.QuarterlyOverview.year == latest_year_overview,
                                               models.QuarterlyOverview.quarter == latest_quarter_overview)
                                    bookval_res = db.execute(overview_bookval).first()[0]

                                    income_statement_epsdil = sqlalch.select(models.QuarterlyIncomeStatement.epsDil). \
                                        filter(models.QuarterlyIncomeStatement.stock_id == ticker_id,
                                               models.QuarterlyIncomeStatement.year == latest_year_statement_income,
                                               models.QuarterlyIncomeStatement.quarter == latest_quarter_statement_income)
                                    epsdil_res = db.execute(income_statement_epsdil).first()[0]

                                    if sharesbasic_res is not None and bookval_res is not None and epsdil_res is not None:
                                        if sharesbasic_res != 0:  # avoid division by zero
                                            before_root = 22.5 * (bookval_res / sharesbasic_res) * epsdil_res  # avoid sqrt of negative number
                                            if before_root >= 0:
                                                graham_value = np.sqrt(before_root)

                                                new_obj = models.GrahamNumber(
                                                    stock_id=ticker_id,
                                                    year=latest_year_balance,
                                                    quarter=latest_quarter_balance,
                                                    graham_value=graham_value
                                                )
                                                db.add(new_obj)
                                                print(f"Graham Number {graham_value} was updated for stock {ticker_name}")
                                                db.commit()
                                else:
                                    print(f"No new update was recorded for stock {ticker_name}")
                        else:
                            print(
                                f"Not able to calculate graham number for stock {ticker_name} due to unmatching quarters")
                    else:
                        print(f"Not able to calculate graham number for stock {ticker_name} due to unmatching years")
                else:
                    print(f"Not able to calculate graham number for stock {ticker_name} due to missing values")
            else:
                print(f"No data was found for stock {ticker_name} in Graham number table")


    def update_pfree_cash_flow_multiplier_table(self) -> None:
        db = get_db()

        # create a dict, in order to have a for loop for all tickers
        stock_name_and_id_dict = self.create_ticker_to_id_dictionary_from_db()
        keys = list(stock_name_and_id_dict.keys())
        for ticker_name in keys:
            ticker_id = stock_name_and_id_dict[ticker_name]

            # retrieve the latest date, year and quarter existing in our db "pfree cash flow" table per stock
            latest_year_and_quarter = self.get_table_latest_year_and_quarter(ticker_id, models.PFreeCashFlowMultiplier)
            latest_date_query = sqlalch.select(models.PFreeCashFlowMultiplier.date). \
                            filter(models.PFreeCashFlowMultiplier.stock_id == ticker_id).order_by(models.PFreeCashFlowMultiplier.date.desc())
            latest_date = db.execute(latest_date_query).first()[0]

            if latest_year_and_quarter and latest_date:

                # extract values from "balance sheet", "overview" and "end of day prices" tables to check if all updated:
                latest_year_and_quarter_balance = self.get_table_latest_year_and_quarter(ticker_id,
                                                                                        models.QuarterlyBalanceSheetData)
                latest_year_and_quarter_cash_flow = self.get_table_latest_year_and_quarter(ticker_id,
                                                                                          models.QuarterlyCashFlow)
                latest_date_eod_prices_query =sqlalch.select(models.EndOfDayPrices.date). \
                            filter(models.EndOfDayPrices.stock_id == ticker_id).order_by(models.EndOfDayPrices.date.desc())
                latest_str_date_eod_prices_res = db.execute(latest_date_eod_prices_query).first()[0]
                latest_date_eod_prices = datetime.strptime(latest_str_date_eod_prices_res, "%Y-%m-%d").date()

                latest_eod_year = int(latest_date_eod_prices.year)
                latest_eod_quarter = pd.Timestamp(latest_date_eod_prices).quarter
                latest_year_balance = latest_year_and_quarter_balance.year
                latest_quarter_balance = latest_year_and_quarter_balance.quarter
                latest_year_cash_flow = latest_year_and_quarter_cash_flow.year
                latest_quarter_cash_flow = latest_year_and_quarter_cash_flow.quarter

                # now check if latest date, year and quarter exist and match in all three tables:
                if latest_year_and_quarter_balance and latest_year_and_quarter_cash_flow and latest_date_eod_prices:
                    if latest_year_balance == latest_year_cash_flow and latest_year_balance == latest_eod_year:
                        if latest_quarter_balance == latest_quarter_cash_flow and latest_quarter_balance == latest_eod_quarter:
                            if latest_date >= latest_date_eod_prices:  # pfree cash flow multiplier cannot be updated
                                print(f"No new data was found for {ticker_name}, pfree cash flow multiplier can't be updated")
                            else:
                                # pfree cash flow multiplier can be updated

                                # Retrieve the latest values necessary for pfree cash flow multiplier calculation:
                                close_price = sqlalch.select([models.EndOfDayPrices.close_price]). \
                                    filter(models.EndOfDayPrices.date == latest_str_date_eod_prices_res,
                                           models.EndOfDayPrices.stock_id == ticker_id)
                                close_res = db.execute(close_price).first()

                                fcf_val = sqlalch.select(models.QuarterlyCashFlow.freeCashFlow). \
                                    filter(models.QuarterlyOverview.stock_id == ticker_id,
                                           models.QuarterlyOverview.year == latest_year_cash_flow,
                                           models.QuarterlyOverview.quarter == latest_quarter_cash_flow)
                                fcf_res = db.execute(fcf_val).first()

                                num_of_shares = sqlalch.select(models.QuarterlyBalanceSheetData.sharesBasic). \
                                    filter(models.QuarterlyIncomeStatement.stock_id == ticker_id,
                                           models.QuarterlyIncomeStatement.year == latest_year_balance,
                                           models.QuarterlyIncomeStatement.quarter == latest_quarter_balance)
                                num_of_shares_res = db.execute(num_of_shares).first()

                                if num_of_shares_res is not None and \
                                        num_of_shares_res[0] is not None and \
                                        fcf_res is not None and fcf_res[0] is not None and close_res is not None:
                                    if fcf_res[0] != 0 and num_of_shares_res[0] != 0:
                                        final_pfcf_ratio_calc = (close_res[0] / (fcf_res[0] / num_of_shares_res[0]))

                                        new_obj = models.PFreeCashFlowMultiplier(
                                            stock_id=ticker_id,
                                            year=latest_eod_year,
                                            quarter=latest_eod_quarter,
                                            pfree_cash_flow_ratio=final_pfcf_ratio_calc
                                        )
                                        db.add(new_obj)
                                        print(f"ticker id:{ticker_id}, date {date}, year {year}, quarter {current_quarter} new row added with {pfree_cash_flow} number")
                                        db.commit()
                                else:
                                    print(f"No new update was recorded for stock {ticker_name}")
                        else:
                            print(f"Not able to calculate pfree cash flow multiplier for stock {ticker_name} due to unmatching quarters")
                    else:
                        print(f"Not able to calculate pfree cash flow multiplier for stock {ticker_name} due to unmatching years")
                else:
                    print(f"Not able to calculate pfree cash flow multiplier for stock {ticker_name} due to missing values")
            else:
                print(f"No data was found for stock {ticker_name} in pfree cash flow multiplier table")

if __name__ == '__main__':

    update_db = UpdateDB()
    db = get_db()

    # update_db.update_end_of_day_prices_table()
    # print("All end of day prices data updated successfully")

    # update_db.update_balance_sheet_table()
    # print("All balance sheet data updated successfully")

    # update_db.update_cash_flow_table()
    # print("All cash flow data updated successfully")

    # update_db.update_income_statement_table()
    # print("All income statement data updated successfully")

    # update_db.update_overview_table()
    # print("All overview data updated successfully")

    # update_db.update_full_daily_multipliers_table()
    # print("All daily multipliers data updated successfully")

    # update_db.update_graham_number_table()
    # print("All daily multipliers data updated successfully")

    # update_db.update_graham_number_table()
    # print("All graham numbers updated successfully")

    # update_db.update_pfree_cash_flow_multiplier_table()
    # print("All pfree cash flow multipliers data updated successfully")

    # print("All tables updated successfully! :))


