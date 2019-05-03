import sqlalchemy


def get_schema():
    return {
        'SERIAL': sqlalchemy.Numeric(precision=15),
        'SHIFT_WT': sqlalchemy.Numeric(precision=9, scale=3),
    }
