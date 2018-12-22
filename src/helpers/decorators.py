import inspect, functools


def decorate_callables(decorator):
    @functools.wraps(decorator)
    def decorate(cls):
        for method_name, method in inspect.getmembers(cls, inspect.isfunction):
            if method_name not in [decorator.__name__, 'set_credentials']:
                setattr(cls, method_name, decorator(method))
        return cls
    return decorate


def clear_credentials(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        result = method(*args, **kwargs)
        if not args[0]._is_transaction:
            args[0].clear_credentials()
        return result
    return wrapper