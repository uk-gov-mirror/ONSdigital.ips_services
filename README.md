# What is the IPS?
The International Passenger Survey (IPS) collects monthly information relating to overseas travel.

# What does the current IPS System do?
The current IPS system calculates weighting factors and imputes missing data for specified periods, using statistical configurations supplied and managed by the users IPS team within Social Surveys Division (SSD).

# Functionality
This package supports the calculations within IPS System. This is also referred to as the "Main Run".
Users select which steps should be executed and then generate reports from the results. These steps can be found under _calculations_ in the **main** folder.

All data resides in the database and code connecting to the database can be found in the _io_ folder. Finally any generic functions are located in the _utils_ folder.

# How to use this package?

Install freetds

# Quick Start

1. Clone and navigate to the root directory of this repository.

2. Ensure python 3 and pip are installed.

3. Install the dependencies with `pip install -r requirements.txt`

4. Create a .env file.

5. Run `docker-compose up`

6. Create tables (see alembic quick start below).

7. Run the ui.


# Alembic

### Quick Start

1. Run the commmand `export PYTHONPATH=<path_to_project>`

2. Add `MYSQL_HOST=<hostname>` to the .env file, this will either be `localhost` or you will need to type `hostname` into the terminal and copy the output.

3. Run all scripts `alembic upgrade head`

### Ammending the migration

1. Run the commmand `alembic revision -m "<migration number> <desctiption of migration>"`, this will create and nam your migration script.

2. Create/alter/update tables under upgrade and remove all changes in the file in the downgrade.

### Migration commands

-  Run the next script `alembic upgrade +1`

-  Peel back one script `alembic downgrade -1`

-  Remove all tables `alembic downgrade base`



