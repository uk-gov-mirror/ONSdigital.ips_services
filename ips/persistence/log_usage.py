from functools import wraps

a = set()


def using_table(table_name):
    def inner_function(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            a.add(table_name)
            function(*args, **kwargs)

        return wrapper

    return inner_function
