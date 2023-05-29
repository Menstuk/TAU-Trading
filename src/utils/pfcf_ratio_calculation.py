import logging
import sqlalchemy as sqlalch
from .database import get_db
from . import models
import pandas as pd
from datetime import datetime


class CalcPFCFMultiplier:

    def __init__(self) -> None:
        super().__init__()

    def get_quarter_by_date(self, date: str) -> int:

        string_to_date = datetime.strptime(date, '%Y-%m-%d').date()
        quarter = pd.Timestamp(string_to_date).quarter
        return quarter

    def check_if_quarter_exists_in_table(self, stock_id: int, db_table: models, date: str, quarter_default = 0) -> bool:

        db = get_db()
        quarter_num = self.get_quarter_by_date(date)
        year = date[0:4]
        query_for_quarter = sqlalch.select([db_table.quarter]).\
                                    filter(db_table.year == year,
                                           db_table.stock_id == stock_id,
                                           db_table.quarter == quarter_num)

        result = db.execute(query_for_quarter).first()

        if result:
            return True
        else:
            return False

    def pfcf_ratio_calc(self, stock_id: int, end_of_day_prices: models.EndOfDayPrices, quarterly_cash_flow_data: models.QuarterlyCashFlow,
                        quarterly_balance_sheet_data: models.QuarterlyBalanceSheetData, date: str,
                        quarter_default=0) -> float | None:
        db = get_db()

        check_for_quarter_balance = self.check_if_quarter_exists_in_table(stock_id, quarterly_balance_sheet_data, date)
        check_for_quarter_cash_flow = self.check_if_quarter_exists_in_table(stock_id, quarterly_cash_flow_data, date)

        if check_for_quarter_balance and check_for_quarter_cash_flow:
            current_quarter = self.get_quarter_by_date(date)
            close_price = sqlalch.select([end_of_day_prices.close_price]).\
                                filter(models.EndOfDayPrices.date == date,
                                       models.EndOfDayPrices.stock_id == stock_id)
            close_res = db.execute(close_price).first()

            fcf_val = sqlalch.select([quarterly_cash_flow_data.freeCashFlow]).\
                filter(models.QuarterlyCashFlow.year == date[0:4],
                       models.QuarterlyCashFlow.quarter == quarter_default,
                       models.QuarterlyCashFlow.stock_id == stock_id)
            fcf_res = db.execute(fcf_val).first()

            num_of_shares = sqlalch.select([quarterly_balance_sheet_data.sharesBasic]).\
                filter(models.QuarterlyBalanceSheetData.year == date[0:4],
                       models.QuarterlyBalanceSheetData.quarter == current_quarter,
                       models.QuarterlyBalanceSheetData.stock_id == stock_id)
            num_of_shares_res = db.execute(num_of_shares).first()
            if num_of_shares_res is not None and\
                    num_of_shares_res[0] is not None and\
                    fcf_res is not None and fcf_res[0] is not None and close_res is not None:
                if fcf_res[0] != 0 and num_of_shares_res[0] != 0:
                    final_pfcf_ratio_calc = (close_res[0] / (fcf_res[0] / num_of_shares_res[0]))
                    return final_pfcf_ratio_calc
                else:
                    return None
        else:
            logging.warning("Quarter doesn't exist in database, unable to complete calculation")
            return None


