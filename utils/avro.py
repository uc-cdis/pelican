import tempfile

from fastavro import writer


def create_avro_from(schema, metadata):
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as avro_output:
        name = avro_output.name
        writer(avro_output, schema, metadata)
    return name, schema
