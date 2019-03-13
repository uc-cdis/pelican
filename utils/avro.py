import json
import tempfile

from fastavro import reader, writer, parse_schema

from utils.str import str_hook


def create_avro_from(avro_file):
    name = None
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as avro_output:
        name = avro_output.name

        with open(avro_file, 'rb') as avro:
            avro_reader = reader(avro)

            avro_schema = avro_reader.writer_schema
            avro_schema = json.loads(json.dumps(avro_schema), object_pairs_hook=str_hook)
            parsed_schema = parse_schema(avro_schema)

            metadata = []
            for record in avro_reader:
                record = {
                    'id': record['id'],
                    'name': record['name'],
                    'object': ('Metadata', record['object']),
                    'relations': []
                }
                metadata.append(record)

        writer(avro_output, parsed_schema, metadata)
    return name, parsed_schema
