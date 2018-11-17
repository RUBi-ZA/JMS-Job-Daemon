from functools import reduce


def deepgetattr(obj, attr, default=None):
    """Recurses through an attribute chain to get the ultimate value."""
    try:
        return reduce(getattr, attr.split('.'), obj)
    except Exception:
        return default

def import_module(module_path):
    """dynamically import the resource manager module"""
    return __import__(module_path, fromlist=["*"])


def import_class(module_path, class_name):
    """Import a class from a given module"""
    module = import_module(module_path)
    return getattr(module, module_path)