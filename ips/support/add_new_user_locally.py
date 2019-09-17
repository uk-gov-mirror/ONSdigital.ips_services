"""Adds new user to local database with hashed password - use for sanity check before using create_new_user.py.
For further information: https://collaborate2.ons.gov.uk/confluence/x/45m3AQ"""

import ips.persistence.users as users

from ips.persistence.persistence import insert_into_table, read_table_values
from werkzeug.security import generate_password_hash, check_password_hash

insert = insert_into_table('USER')
get_users = read_table_values('USER')


def __validate_username(user_name: str) -> bool:
    # Check username doesn't already exist
    username_records = get_users()

    if user_name in username_records['USER_NAME'].unique():
        print("Username already exists. Please use a different username.")
        return False

    return True


def __validate_db_update(uname: str, pwd: str) -> bool:
    # Validate database was updated successfully with the correct password
    user_credentials = users.get_user_details(uname)
    if user_credentials.empty:
        print(f"User, {uname}, not added.")
        return False

    if not check_password_hash(user_credentials['PASSWORD'].values[0], pwd):
        print("Corrupted password.")
        return False

    print("Successfully updated.")
    return True


def add_new_user_locally(first_name: str, surname: str, role: str, username: str, str_pwd: str) -> bool:
    # Sanity check new user credentials by adding them to your local db
    if not __validate_username(username):
        return False

    # input to database
    encoded_pwd = str(generate_password_hash(str_pwd))
    insert(USER_NAME=username, PASSWORD=encoded_pwd, FIRST_NAME=first_name, SURNAME=surname, ROLE=role)

    if not __validate_db_update(username, str_pwd):
        return False

    print(f"Database updated. Username: '{username}', password: '{str_pwd}'")
    return True


if __name__ == '__main__':
    """REQUIRED INPUTS:"""
    first_name = ''
    surname = ''
    role = ''
    username = ''
    str_pwd = ''

    add_new_user_locally(first_name, surname, role, username, str_pwd)
