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

1. Clone and navigate to the root directory of this repository.

2. Ensure python 3 and pip are installed.

3. Create a .env file and populate with environment variables (see below)

4. Run `docke-compose build`

5. Run `docker-compose up`

6. Create tables (see alembic quick start below).

7. Run the ui.


##Alembic

#### Quick Start
From the project root folder:

1. Run the commmand `export PYTHONPATH=<path_to_project>`

2. Add `MYSQL_HOST=<hostname>` to the .env file, hostname will either be `localhost` or you will need to type `hostname` into the terminal and copy the output.

3. Run all scripts `alembic upgrade head`

#### Ammending the migration

1. Run the commmand `alembic revision -m "<migration number> <desctiption of migration>"`, this will create and nam your migration script.

2. Create/alter/update tables under upgrade and remove all changes in the file in the downgrade.

#### Migration commands

-  Run the next script `alembic upgrade +1`

-  Peel back one script `alembic downgrade -1`

-  Remove all tables `alembic downgrade base`

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


