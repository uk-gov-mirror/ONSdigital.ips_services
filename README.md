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

6. Navigate to localhost:5001, this will show the UI


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
DB_USER_NAME=ips
DB_PASSWORD=ips
DB_NAME=ips
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=ips
MYSQL_PASSWORD=ips
MYSQL_USER=ips
FLASK_APP=myapp
FLASK_ENV=development
UI_FLASK_APP=ips


