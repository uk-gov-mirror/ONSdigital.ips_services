import pandas as pd
from ips_common.ips_logging import log


def validate_data(reference_type: str, data: pd.DataFrame, user_month: str, user_year: str, errors):

    data['DATASOURCE'] = data['DATASOURCE'].str.upper()

    for index, row in data.iterrows():
        datasource = row['DATASOURCE']

        if reference_type not in datasource:
            log.error(f"Data does not match {reference_type}. Exiting validation.")
            errors.add(f"Data does not match {reference_type}.")
            return False

    return _validate_date(data, user_month, user_year, errors)


def _validate_date(data, user_month, user_year, errors):
    valid_quarters = {
        "Q1": ['1', '2', '3'], "Q2": ['4', '5', '6'], "Q3": ['7', '8', '9'], "Q4": ['10', '11', '12']
    }

    for index, row in data.iterrows():
        year = str(row['YEAR'])

        if not str.isdigit(year) or not 2000 <= int(year) <= 2099:
            errors.add(f"year value [{year}] in survey data stream is invalid")
            return False

        if not str.isdigit(user_year) or int(user_year) != int(year):
            errors.add(f"user supplier year value [{user_year}] is invalid")
            return False

    for index, row in data.iterrows():
        month = str(row['MONTH'])

        if user_month in valid_quarters and month not in valid_quarters[user_month]:
            errors.add(f"user supplied quarter [{user_month}] does not correspond to valid month in data [{month}]")
            return False

        if not str.isdigit(month) or not 1 <= int(month) <= 12:
            errors.add(f"data month value [{month}] in data stream is invalid")
            return False

        if int(month) != int(user_month):
            errors.add(f"user supplied month [{user_month}] does not correspond to data month [{month}]")
            return False

    return True
