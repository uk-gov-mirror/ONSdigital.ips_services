from ips.persistence.persistence import read_table_values

get_users = read_table_values('USER')


def get_user_details(user_name: str):
    data = get_users()
    return data.loc[data['USER_NAME'] == user_name]
