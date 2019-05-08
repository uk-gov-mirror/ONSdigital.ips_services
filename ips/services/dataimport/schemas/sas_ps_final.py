import sqlalchemy


def get_schema():
    return {
        'SERIAL': sqlalchemy.Numeric(precision=15),
        'SHIFT_WT': sqlalchemy.Numeric(precision=9, scale=3),
        'NON_RESPONSE_WT': sqlalchemy.Numeric(precision=9, scale=3),
        'MINS_WT': sqlalchemy.Numeric(precision=9, scale=3),
        'TRAFFIC_WT': sqlalchemy.Numeric(precision=9, scale=3),
        'UNSAMP_TRAFFIC_WT': sqlalchemy.Numeric(precision=9, scale=3),
        'IMBAL_WT': sqlalchemy.Numeric(precision=9, scale=3),
        'FINAL_WT': sqlalchemy.Numeric(precision=12, scale=3),

    }
