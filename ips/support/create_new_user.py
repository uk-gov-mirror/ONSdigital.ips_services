"""Generates SQL statement with hashed password to update database.
For further information: https://collaborate2.ons.gov.uk/confluence/x/45m3AQ"""

from werkzeug.security import generate_password_hash


def __encrypt_password(pwd: str) -> str:
    # Generate the hashed password
    return str(generate_password_hash(pwd))


def insert_statement(user_name: str, str_pwd: str, first_name: str, surname: str, role: str) -> str:
    # Generate string of sql statement to insert *new* user credentials to USER table
    if not str_pwd.strip():
        print("Error: Please provide a value for str_pwd.")
        return ""

    encrypted_pwd = __encrypt_password(str_pwd)
    sql = f"""INSERT INTO USER(ID, USER_NAME, PASSWORD, FIRST_NAME, SURNAME, ROLE)
                VALUES('', '{user_name}', '{encrypted_pwd}', '{first_name}', '{surname}', '{role}');"""

    return sql


def update_statement(user_name: str, str_pwd: str) -> str:
    # Generate string of sql statement to insert *new* user credentials to USER table
    if not str_pwd.strip():
        print("Error: Please provide a value for str_pwd.")
        return ""

    encrypted_pwd = __encrypt_password(str_pwd)

    sql = f"""UPDATE USER SET PASSWORD = '{encrypted_pwd}' WHERE USER_NAME = '{user_name}';"""

    return sql


if __name__ == '__main__':
    """REQUIRED INPUTS:"""
    first_name = ''
    surname = ''
    role = ''
    username = ''
    str_pwd = ''

    # print(insert_statement(username, str_pwd, first_name, surname, role))
    print(update_statement(username, str_pwd))

