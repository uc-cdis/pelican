import base64


def encode(raw_value):
    return base64.b64encode(raw_value).rstrip("=")


def decode(encoded_value):
    return base64.b64decode(encoded_value).rstrip("=")
