# this file refers to SQLAlchemy models
from typing import Any
from sqlalchemy import Column, Integer, String, Float, ForeignKey
import utils.database as database
from utils.database import Base
from sqlalchemy.orm import relationship

# All models below comprise the stock_database tables that hold the relevant data extracted from Tiingo's website
# (SQLAlchemy ORM -  presents a method of associating user-defined Python classes with database tables,
# and instances of those classes (objects) with rows in their corresponding tables)


class EndOfDayPricesWithNull(Base):
    __tablename__ = 'end_of_day_prices_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    date = Column(String(16))
    close_price = Column(Float)


class GrahamNumberWithNull(Base):
    __tablename__ = 'graham_number_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    year = Column(Integer)
    quarter = Column(Integer)
    graham_value = Column(Float, nullable=True)


class PFreeCashFlowMultiplierWithNull(Base):
    __tablename__ = 'p_free_cash_flow_multiplier_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    date = Column(String(16))
    year = Column(Integer)
    quarter = Column(Integer)
    pfree_cash_flow_ratio = Column(Float)


class FullDailyMultipliersWithNull(Base):
    __tablename__ = 'full_daily_multipliers_with_null'

    id = Column(Integer, primary_key=True, index=True)
    date = Column(String(16))
    stock_id = Column(Integer)
    market_cap = Column(Float)
    enterprise_val = Column(Float)
    pe_ratio = Column(Float)
    pb_ratio = Column(Float)
    trailing_peg_1_y = Column(Float)


class QuarterlyBalanceSheetDataWithNull(Base):
    __tablename__ = 'quarterly_balance_sheet_data_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    date = Column(String(16))
    year = Column(Integer)
    quarter = Column(Integer)
    debtCurrent = Column(Float)
    taxAssets = Column(Float)
    investmentsCurrent = Column(Float)
    totalAssets = Column(Float)
    acctPay = Column(Float)
    accoci = Column(Float)
    inventory = Column(Float)
    totalLiabilities = Column(Float)
    acctRec = Column(Float)
    intangibles = Column(Float)
    ppeq = Column(Float)
    deferredRev = Column(Float)
    cashAndEq = Column(Float)
    assetsNonCurrent = Column(Float)
    taxLiabilities = Column(Float)
    investments = Column(Float)
    equity = Column(Float)
    retainedEarnings = Column(Float)
    deposits = Column(Float)
    assetsCurrent = Column(Float)
    investmentsNonCurrent = Column(Float)
    debt = Column(Float)
    debtNonCurrent = Column(Float)
    liabilitiesNonCurrent = Column(Float)
    liabilitiesCurrent = Column(Float)
    sharesBasic = Column(Float)


class QuarterlyCashFlowWithNull(Base):
    __tablename__ = 'quarterly_cash_flow_data_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    date = Column(String(16))
    year = Column(Integer)
    quarter = Column(Integer)
    ncfi = Column(Float)
    capex = Column(Float)
    ncfx = Column(Float)
    ncff = Column(Float)
    sbcomp = Column(Float)
    ncf = Column(Float)
    payDiv = Column(Float)
    businessAcqDisposals = Column(Float)
    issrepayDebt = Column(Float)
    issrepayEquity = Column(Float)
    investmentsAcqDisposals = Column(Float)
    freeCashFlow = Column(Float)
    ncfo = Column(Float)
    depamor = Column(Float)


class QuarterlyIncomeStatementWithNull(Base):
    __tablename__ = 'quarterly_income_statement_data_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    date = Column(String(16))
    year = Column(Integer)
    quarter = Column(Integer)
    ebit = Column(Float)
    epsDil = Column(Float)
    rnd = Column(Float)
    shareswa = Column(Float)
    taxExp = Column(Float)
    opinc = Column(Float)
    costRev = Column(Float)
    grossProfit = Column(Float)
    ebitda = Column(Float)
    nonControllingInterests = Column(Float)
    netIncDiscOps = Column(Float)
    eps = Column(Float)
    intexp = Column(Float)
    shareswaDil = Column(Float)
    revenue = Column(Float)
    netinc = Column(Float)
    opex = Column(Float)
    consolidatedIncome = Column(Float)
    netIncComStock = Column(Float)
    ebt = Column(Float)
    prefDVDs = Column(Float)
    sga = Column(Float)


class QuarterlyOverviewWithNull(Base):
    __tablename__ = 'quarterly_overview_data_with_null'

    id = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer)
    date = Column(String(16))
    year = Column(Integer)
    quarter = Column(Integer)
    longTermDebtEquity = Column(Float)
    shareFactor = Column(Float)
    bookVal = Column(Float)
    roa = Column(Float)
    currentRatio = Column(Float)
    roe = Column(Float)
    grossMargin = Column(Float)
    piotroskiFScore = Column(Float)
    epsQoQ = Column(Float)
    revenueQoQ = Column(Float)
    profitMargin = Column(Float)
    rps = Column(Float)
    bvps = Column(Float)


def create_tables_for_all_models_with_null():
    Base.metadata.create_all(bind=database.engine)
