import sys
import logging
import pandas as pd
import numpy as np
import sqlalchemy as sqlalch
from utils.combined_data_for_graham_calculation import CombinedDataForGrahamCalculation
from utils.database import get_db
from utils import models
from utils.tiingo_api import TiingoApi
from utils.pfcf_ratio_calculation import CalcPFCFMultiplier

SNP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
SNP500_WIKI_SYMBOL_COLUMN_NAME = 'Symbol'


class PopulateDB:

    t_api = TiingoApi()
    pfcf_mult_calc = CalcPFCFMultiplier()
    ticker_name_to_id_dict: dict[str, int]

    def __init__(self) -> None:
        super().__init__()

        db = get_db()
        num_of_stocks = db.query(models.StocksByID).count()
        logging.info(f"{num_of_stocks} exist in DB")
        if num_of_stocks == 0:
            self.populate_stock_by_id_table()

        self.ticker_name_to_id_dict = self.create_ticker_to_id_dictionary_from_db()
        self.dates_list = self.create_dates_list()

    def populate_stock_by_id_table(self) -> None:

        # fetching the relevant table, containing the s&p500 tickers, from wikipedia
        datatable_snp500 = pd.read_html(SNP500_WIKI_URL)[0]

        # selecting only symbols' columns, converting them into a list
        snp500_tickers_lst = datatable_snp500[SNP500_WIKI_SYMBOL_COLUMN_NAME].tolist()

        db = get_db()

        models_lst = map(lambda ticker_name: models.StocksByID(ticker_name), snp500_tickers_lst)
        db.bulk_save_objects(models_lst)
        db.commit()

    def create_ticker_to_id_dictionary_from_db(self) -> dict[str, int]:
        db = get_db()

        results = db.query(models.StocksByID).all()
        dict1 = {}
        for stock_by_id_item in results:
            dict1[stock_by_id_item.stock_name] = stock_by_id_item.id

        return dict1

    def create_dates_list(self) -> list:

        db = get_db()

        all_dates_list = []
        dates_res = list(db.query(models.EndOfDayPrices.date).distinct().all())
        for date in dates_res:
            date = date[0]
            all_dates_list.append(date)

        return all_dates_list

    # This function extracts the end of day prices data of all S&P 500 stocks and populates "end of day prices" table in db
    def populate_end_of_day_prices(self) -> None:

        db = get_db()
        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]
            end_of_day_list = self.t_api.get_end_of_day_prices_by_date(ticker_name)
            for end_of_day_item in end_of_day_list:
                model = models.EndOfDayPrices(stock_id=ticker_id, date=end_of_day_item.date[:10],
                                              close_price=end_of_day_item.close)
                db.add(model)

        db.commit()

    # This function extracts the balance sheet data from the full fundamentals json of all S&P 500 stocks
    def populate_stock_balance_sheet(self) -> None:
        db = get_db()

        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]
            fundamentals_lst = self.t_api.get_all_daily_fundamentals_data(ticker_name)

            for fundamental_item in fundamentals_lst:
                # create a dictionary from array
                dict1 = {}

                # Populate dictionary with data from balance sheet.
                # f there is no balance sheet then dictionary will be empty and rows will be populated with None values
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

    def populate_cash_flow(self) -> None:
        db = get_db()

        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]
            fundamentals_lst = self.t_api.get_all_daily_fundamentals_data(ticker_name)

            for fundamental_item in fundamentals_lst:
                # create a dictionary from array
                dict1 = {}

                # Populate dictionary with data from cash flow table.
                # If there is no cash flow table then dictionary will be empty and rows will be populated with None values
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

    def populate_stock_income_statement(self) -> None:
        db = get_db()

        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]
            fundamentals_lst = self.t_api.get_all_daily_fundamentals_data(ticker_name)

            for fundamental_item in fundamentals_lst:
                # create a dictionary from array
                dict1 = {}


                # Populate dictionary with data from income statement table.
                # If there is no income statement table then dictionary will be empty and rows will be populated with None values
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

    def populate_overview(self) -> None:
        db = get_db()

        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]
            fundamentals_lst = self.t_api.get_all_daily_fundamentals_data(ticker_name)

            for fundamental_item in fundamentals_lst:
                # create a dictionary from array
                dict1 = {}

                # Populate dictionary with data from overview table.
                # If there is no overview table then dictionary will be empty and rows will be populated with None values
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

    # Function below calculates the graham number per each S&P 500 stock
    def populate_graham_table(self):

        missing_quarters_count = 0

        db = get_db()
        for stock_id in CombinedDataForGrahamCalculation.my_dict.keys():
            year_dict = CombinedDataForGrahamCalculation.my_dict[stock_id]
            for year in year_dict.keys():
                for quarter_index in range(1, 5):
                    graham_value = None
                    quarter_data = year_dict[year][quarter_index]
                    if quarter_data is not None:
                        if quarter_data.book_val is not None and quarter_data.shares_basic is not None and quarter_data.eps_dil is not None:
                            if quarter_data.shares_basic != 0:  # avoid division by zero
                                before_root = 22.5 * (quarter_data.book_val / quarter_data.shares_basic) * quarter_data.eps_dil # avoid sqrt of negative number
                                if before_root >= 0:
                                    graham_value = np.sqrt(before_root)
                        logging.debug(f'stock_id: {stock_id}, year: {year}, quarter: {quarter_index}, graham_value: {graham_value}')
                        new_obj = models.GrahamNumber(
                            stock_id=stock_id,
                            year=year,
                            quarter=quarter_index,
                            graham_value=graham_value
                        )
                        db.add(new_obj)
                    else:
                        missing_quarters_count += 1
        db.commit()

    def populate_pfree_cash_flow(self) -> None:
        db = get_db()

        dates_list = self.create_dates_list()
        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]

            for date in dates_list:
                pfree_cash_flow = self.pfcf_mult_calc.pfcf_ratio_calc(ticker_id,
                                                                  models.EndOfDayPrices,
                                                                  models.QuarterlyCashFlow,
                                                                  models.QuarterlyBalanceSheetData, date)
                current_quarter = self.pfcf_mult_calc.get_quarter_by_date(date)
                year = date[:4]

                new_obj = models.PFreeCashFlowMultiplier(
                    stock_id=ticker_id,
                    year=year,
                    quarter=current_quarter,
                    pfree_cash_flow_ratio=pfree_cash_flow

                )
                db.add(new_obj)
                print(f"ticker id:{ticker_id}, date {date}, year {year}, quarter {current_quarter} new row added with {pfree_cash_flow} number")

            db.commit()

    # function below updates the table full_daily_multipliers table without foreign keys from
    # "pfree cash flow" and "end of day prices" tables
    def populate_full_daily_multipliers(self) -> None:
        db = get_db()

        keys = list(self.ticker_name_to_id_dict.keys())
        for ticker_name in keys:
            ticker_id = self.ticker_name_to_id_dict[ticker_name]
            daily_multipliers_lst = self.t_api.get_daily_multipliers(ticker_name)

            for daily_item in daily_multipliers_lst:
                date_str = daily_item.date[:10]

                new_obj = models.FullDailyMultipliers(
                    stock_id=ticker_id,
                    date=date_str,
                    market_cap=daily_item.marketCap,
                    enterprise_val=daily_item.enterpriseVal,
                    pe_ratio=daily_item.peRatio,
                    pb_ratio=daily_item.pbRatio,
                    trailing_peg_1_y=daily_item.trailingPEG1Y,
                )
                db.add(new_obj)
                print(f"new row add with ticker id:{ticker_id}, date: {date_str}, pb_ratio: {daily_item.pbRatio}")
            db.commit()



if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

    populate_db = PopulateDB()

    # populate_db.populate_stock_by_id_table
    # print("finished stock by id update")

    # populate_db.populate_stock_balance_sheet()
    # print("finished balance sheet update")

    # populate_db.populate_end_of_day_prices()
    # print("finished end of day prices update")

    # populate_db.populate_cash_flow()
    # print("finished cash flow update")

    # populate_db.populate_stock_income_statement()
    # print("finished income statement update")

    # populate_db.populate_overview()
    # print("finished overview update")

    # populate_db.populate_full_daily_multipliers()
    # print("finished full_daily_multipliers update")

    # populate_db.populate_pfree_cash_flow()
    # print("finished pfree cash flow update")

    # db = get_db()
    # comb = CombinedDataForGrahamCalculation()
    # comb.fill_combined_data(
    #     db.query(models.QuarterlyBalanceSheetData).all(),
    #     db.query(models.QuarterlyOverview).all(),
    #     db.query(models.QuarterlyIncomeStatement).all()
    #  )
    # populate_db.populate_graham_table()
    # print("finished graham number table update")



