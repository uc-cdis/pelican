import json
import tempfile

from fastavro import reader, writer, parse_schema

from utils.str import str_hook


def create_avro_from(schema, metadata):
    name = None
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as avro_output:
        name = avro_output.name
        writer(avro_output, schema, metadata)
    return name, schema
