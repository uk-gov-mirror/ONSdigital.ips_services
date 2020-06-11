import pandas as pd
from ips.util.services_logging import log


def validate_survey_data(data: pd.DataFrame, user_month, user_year, errors):
    log.info("Validating Survey data...")

    if 'SERIAL' not in data.columns:
        log.error(f"'SERIAL' column does not exist. Exiting validation.")
        errors.add("'SERIAL' column does not exist in Survey data.")
        return False

    if 'INTDATE' not in data.columns:
        log.error("'INTDATE' column does not exist. Exiting validation.")
        errors.add("'INTDATE' column does not exist in Survey data.")
        return False

    if user_month is None or user_year is None:
        return True

    return _validate_date(data, user_month, user_year, errors, data_type='Survey')


def validate_reference_data(reference_type: str, data: pd.DataFrame, user_month: str, user_year: str, errors):
    log.info(f"Validating {reference_type} data...")
    data.columns = data.columns.str.upper()
    data.columns = data.columns.str.replace(' ', '')

    if 'DATASOURCE' not in data.columns:
        errors.add(f"Invalid {reference_type} data uploaded")

    data['DATASOURCE'] = data['DATASOURCE'].str.replace(' ', '')

    for index, row in data.iterrows():
        datasource = row['DATASOURCE']

        if reference_type not in datasource:
            errors.add(f"{datasource} uploaded instead of {reference_type}. Exiting validation.")
            return False

    if user_month is None or user_year is None:
        return True

    return _validate_date(data, user_month, user_year, errors, data_type=reference_type)


def _validate_date(data, user_month, user_year, errors, data_type):
    valid_quarters = {
        "Q1": ['1', '2', '3'], "Q2": ['4', '5', '6'],
        "Q3": ['7', '8', '9'], "Q4": ['10', '11', '12']
    }

    quarters_found: set = set()

    def valid_year():
        if not str.isdigit(year) or not 2000 <= int(year) <= 2099:
            errors.add(f"year value [{year}] in {data_type} is invalid")
            return False

        if not str.isdigit(user_year) or int(user_year) != int(year):
            errors.add(f"user supplier year value [{user_year}] is invalid")
            return False
        return True

    def valid_month():
        if str.isdigit(user_month):
            if int(month) != int(user_month):
                errors.add(f"user supplied month [{user_month}] does not correspond to {data_type} month [{month}]")
                return False

            if not 1 <= int(month) <= 12:
                errors.add(f"data month value [{month}] in {data_type} is invalid")
                return False
        else:
            if user_month in valid_quarters:
                if month not in valid_quarters[user_month]:
                    errors.add(
                        f"user supplied quarter [{user_month}] does not correspond to valid month in {data_type} [{month}]"
                    )
                    return False
                quarters_found.add(month)
            else:
                errors.add(f"[{user_month}] is not a valid quarter")
                return False
        return True

    if 'INTDATE' in data.columns:
        date_column = data['INTDATE'].astype(str).str.rjust(8, '0')

        for index, row in date_column.iteritems():
            year = row[-4:]
            month = row[-6:][:2]
            if month[0] == '0':
                month = month[1:]

            if not valid_year() or not valid_month():
                return False
    else:
        for index, row in data.iterrows():
            year = str(row['YEAR'])
            month = str(row['MONTH'])

            if not valid_year() or not valid_month():
                return False

    if user_month in valid_quarters:
        qf = list(quarters_found)
        for x in qf:
            if x not in valid_quarters[user_month]:
                errors.add(f"{data_type} for the quarter, [{user_month}], does not contain all valid months")
                return False

    return True
