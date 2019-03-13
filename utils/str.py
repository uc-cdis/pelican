# https://stackoverflow.com/a/42377964/1030110
def str_hook(obj):
    return {k.encode('utf-8') if isinstance(k, unicode) else k: v.encode('utf-8') if isinstance(v, unicode) else v for
            k, v in obj}
