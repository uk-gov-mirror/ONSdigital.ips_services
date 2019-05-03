import sqlalchemy


def get_schema():
    return {
        'SERIAL': sqlalchemy.Numeric(precision=15),
        'VISIT_WT': sqlalchemy.Numeric(precision=6, scale=3),
        'STAY_WT': sqlalchemy.Numeric(precision=6, scale=3),
        'EXPENDITURE_WT': sqlalchemy.Numeric(precision=6, scale=3),
        'VISIT_WTK': sqlalchemy.NVARCHAR,
        'STAY_WTK': sqlalchemy.NVARCHAR,
        'EXPENDITURE_WTK': sqlalchemy.NVARCHAR,
        'NIGHTS1': sqlalchemy.Numeric(precision=3),
        'NIGHTS2': sqlalchemy.Numeric(precision=3),
        'NIGHTS3': sqlalchemy.Numeric(precision=3),
        'NIGHTS4': sqlalchemy.Numeric(precision=3),
        'NIGHTS5': sqlalchemy.Numeric(precision=3),
        'NIGHTS6': sqlalchemy.Numeric(precision=3),
        'NIGHTS7': sqlalchemy.Numeric(precision=3),
        'NIGHTS8': sqlalchemy.Numeric(precision=3),
        'STAY1K': sqlalchemy.NVARCHAR,
        'STAY2K': sqlalchemy.NVARCHAR,
        'STAY3K': sqlalchemy.NVARCHAR,
        'STAY4K': sqlalchemy.NVARCHAR,
        'STAY5K': sqlalchemy.NVARCHAR,
        'STAY6K': sqlalchemy.NVARCHAR,
        'STAY7K': sqlalchemy.NVARCHAR,
        'STAY8K': sqlalchemy.NVARCHAR,
    }
