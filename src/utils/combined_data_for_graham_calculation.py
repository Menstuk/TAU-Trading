from dataclasses import dataclass
import models


@dataclass
class QuarterlyCombined:
    eps_dil: float
    book_val: float
    shares_basic: float


class CombinedDataForGrahamCalculation:

    my_dict: dict[int, dict[int, list[QuarterlyCombined | None]]] = {}

    # public
    def get_quarterly_combined_item(self, stock_id: int, year: int, quarter: int) -> QuarterlyCombined:
        year_dict = self.my_dict.get(stock_id)
        if year_dict is None:
            year_dict: dict[int, list[QuarterlyCombined | None]] = {}
            self.my_dict[stock_id] = year_dict

        quarters_lst: list[QuarterlyCombined | None] = year_dict.get(year)
        if quarters_lst is None:
            quarters_lst = [None] * 5
            year_dict[year] = quarters_lst

        item = quarters_lst[quarter]
        if item is None:
            item = QuarterlyCombined(None, None, None)
            quarters_lst[quarter] = item
        return quarters_lst[quarter]

    # public
    def fill_combined_data(
            self,
            balance_sheets_lst: list[models.QuarterlyBalanceSheetData],
            overview_lst: list[models.QuarterlyOverview],
            income_statement_lst: list[models.QuarterlyIncomeStatement]):

        for balance_sheet in balance_sheets_lst:
            q_data = self.get_quarterly_combined_item(
                balance_sheet.stock_id,
                balance_sheet.year,
                balance_sheet.quarter
            )
            q_data.shares_basic = balance_sheet.sharesBasic

        for overview in overview_lst:
            q_data = self.get_quarterly_combined_item(
                overview.stock_id,
                overview.year,
                overview.quarter
            )
            q_data.book_val = overview.bookVal

        for income_statement in income_statement_lst:
            if income_statement.quarter == 0:
                for i in range(0, 5):
                    q_data = self.get_quarterly_combined_item(
                        income_statement.stock_id,
                        income_statement.year,
                        i
                    )
                    q_data.eps_dil = income_statement.epsDil
