import re

__ROUTES = dict()


def route(path):
    def decorator(fn):
        __ROUTES[path] = fn
        return fn
    return decorator


def route_is_defined(path):
    fn, _ = match_route(path)
    return fn is not None


def dispatch(path, *args, **kwargs):
    fn, path_params = match_route(path)
    kwargs.update(path_params)
    return fn(*args, **kwargs)


def match_route(path):
    if path in __ROUTES:
        return __ROUTES[path], {}

    def make_pattern(route):
        return "^" + re.sub(r"{(\w+)}", r"(?P<\1>[^/]+)", route) + "$"

    for key, value in __ROUTES.items():
        match = re.match(make_pattern(key), path)

        if match is None:
            continue

        return value, match.groupdict()

    return None, None
