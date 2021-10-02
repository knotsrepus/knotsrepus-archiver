__ROUTES = dict()


def route(path):
    def decorator(fn):
        __ROUTES[path] = fn
        return fn
    return decorator


def route_is_defined(path):
    return path in __ROUTES


def dispatch(path, *args, **kwargs):
    fn = __ROUTES.get(path)
    return fn(*args, **kwargs)
