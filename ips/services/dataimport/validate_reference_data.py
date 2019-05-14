import pandas as pd
from ips_common.ips_logging import log


def validate_data(reference_type: str, data: pd.DataFrame, user_month: str, user_year: str, errors):
    data.columns = data.columns.str.upper()
    data.columns = data.columns.str.replace(' ', '')
    data['DATASOURCE'] = data['DATASOURCE'].str.replace(' ', '')

    for index, row in data.iterrows():
        datasource = row['DATASOURCE']

        if reference_type not in datasource:
            log.error(f"Data does not match {reference_type}. Exiting validation.")
            errors.add(f"Data does not match {reference_type}.")
            return False

    if user_month is None or user_year is None:
        return True

    return _validate_date(data, user_month, user_year, errors)


def _validate_date(data, user_month, user_year, errors):
    valid_quarters = {
        "Q1": ['1', '2', '3'], "Q2": ['4', '5', '6'],
        "Q3": ['7', '8', '9'], "Q4": ['10', '11', '12']
    }

    quarters_found: set = set()

    def valid_year():

        if not str.isdigit(year) or not 2000 <= int(year) <= 2099:
            errors.add(f"year value [{year}] in data stream is invalid")
            return False

        if not str.isdigit(user_year) or int(user_year) != int(year):
            errors.add(f"user supplier year value [{user_year}] is invalid")
            return False
        return True

    def valid_month():
        if str.isdigit(user_month):
            if int(month) != int(user_month):
                errors.add(f"user supplied month [{user_month}] does not correspond to data month [{month}]")
                return False

            if not 1 <= int(month) <= 12:
                errors.add(f"data month value [{month}] in data stream is invalid")
                return False
        else:
            if user_month in valid_quarters:
                if month not in valid_quarters[user_month]:
                    errors.add(
                        f"user supplied quarter [{user_month}] does not correspond to valid month in data [{month}]"
                    )
                    return False
                quarters_found.add(month)
            else:
                errors.add(f"[{user_month}] is not a valid quarter")
                return False
        return True

    for index, row in data.iterrows():
        year = str(row['YEAR'])
        month = str(row['MONTH'])

        if not valid_year() or not valid_month():
            return False

    if user_month in valid_quarters:
        qf = list(quarters_found).sort()
        if qf != valid_quarters[user_month]:
            errors.add(f"Data for the quarter, [{user_month}], does not contain all valid months")
            return False

    return True
