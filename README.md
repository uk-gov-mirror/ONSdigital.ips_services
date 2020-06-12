# What is the IPS?
The International Passenger Survey (IPS) collects monthly information relating to overseas travel.

# What does the current IPS System do?
The current IPS system calculates weighting factors and imputes missing data for specified periods, using statistical configurations supplied and managed by the users IPS team within Social Surveys Division (SSD).

# Functionality
This package supports the calculations within IPS System. This is also referred to as the "Main Run".
Users select which steps should be executed and then generate reports from the results. These steps can be found under _calculations_ in the **main** folder.

All data resides in the database and code connecting to the database can be found in the _io_ folder. Finally any generic functions are located in the _utils_ folder.

# How to use this package?
## Quick Start

1. Clone UI project: https://github.com/ONSdigital/ips_user_interface

2. Clone services repository and navigate to the root directory of this repository.

2. Ensure python 3 and pip are installed.

3. Create a .env file and populate with environment variables (see below)

4. Run `docker-compose build`

5. Run `docker-compose up` (This will set up UI, Services & DB)

6. Run ips_services:
    * Configure new Python run
    * In <i>Script path</i> select /ips_services/venv/bin/waitress-serve
    * In <i>Parameters</i> add `--listen=*:5000 --threads=8 ips.app:app`
    * In <i>Environment variables</i> add the following: 
        DB_SERVER=localhost
        DB_USER_NAME=<REDACTED>
        DB_PASSWORD=<REDACTED>
        DB_NAME=<REDACTED>
    * Ensure the <i>Python interpreter</i> is configured

7. Run ips_user_interface:
    * Configure new Flask server
    * In <i>Target</i> add ips
    * In <i>Additional options</i> add `--port=5001`
    * In <i>FLASK_ENV</i> add `development`
    * In <i>Environment variables</i> add the following: 
        API_HOST=localhost
        API_PORT=5000
        API_PROTOCOL=http
        LOG_CONSOLE_LEVEL=DEBUG
    * Ensure the <i>Python interpreter</i> is configured
    
8. Navigate to localhost:5001, this will show the UI


## Alembic

#### [Confluence Documentation](https://collaborate2.ons.gov.uk/confluence/display/QSS/Alembic+Database+Migration)

#### Quick Start
The database is currently being created and populated in ips_services/db/data/ips_mysql_schema.sql 

To create in alembic the sql file needs to be commented out after line 5, then run `docker-compose down -v` and repeat steps 4 and 5 above followed by the three steps directly below.

From the project root folder:

1. Run the commmand `export PYTHONPATH=<path_to_project>`

2. Add `MYSQL_HOST=<hostname>` to the .env file, hostname will either be `localhost` or you will need to type `hostname` into the terminal and copy the output.

3. Run build scripts `alembic upgrade 7fe61c4343bb`

#### Adding Tables/Columns/Data

There us an 'update tables' script already created, it contains an example of adding a column to the user table.

Commands can be added to this file to update the tables.

Make sure to reflect the upgrades in the downgrade function.

#### Migration commands

-  Run all upgrade scripts `alembic upgrade head`

-  Run the next script upgrade `alembic upgrade +1`

-  Run the downgrade script `alembic downgrade -1`

-  Remove all tables `alembic downgrade base`

There is documentation for creating and editing tables in the confluence page linked above

#### Environment Variables

API_HOST=ips-services
API_PORT=5000
API_PROTOCOL=http
DB_SERVER=ips-db
DB_USER_NAME=<REDACTED>
DB_PASSWORD=<REDACTED>
DB_NAME=<REDACTED>
MYSQL_ROOT_PASSWORD=<REDACTED>
MYSQL_DATABASE=<REDACTED>
MYSQL_PASSWORD=<REDACTED>
MYSQL_USER=<REDACTEED>
FLASK_APP=myapp
FLASK_ENV=development
UI_FLASK_APP=<REDACTED>

#### R Set Up: Local

Ensure you have R installed and run following command in terminal:

`R CMD INSTALL r-packages/ReGenesees_1.9.tar.gz && \
R CMD INSTALL r-packages/DBI_1.0.0.tar.gz && \
R CMD INSTALL r-packages/RMySQL_0.10.17.tar.gz`
    






