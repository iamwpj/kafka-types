import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder
import io
from typing import List


class Schema:
    def __init__(self, schema_name: str) -> io.BytesIO | None:
        self.schema = avro.schema.parse(
            open(f"./schemas/{schema_name}.avsc", "rb").read()
        )

    def apply(self, data: List[dict]) -> bytes:
        """Apply a schema to provided list of data.

        Args:
            data (List[dict]): A list containing JSON data.

        Returns:
            bytes: This is the serialized schema data. It can be read by supplying to the read_out funciton.
        """
        writer = DatumWriter(self.schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)

        [writer.write(x, encoder=encoder) for x in data]

        return bytes_writer.getvalue()

    def read_out(self, data) -> str:
        """Provide the data from the apply function above.

        Returns:
            str: JSON/dict/string output of the data.
        """

        decoder = BinaryDecoder(io.BytesIO(data))
        reader = DatumReader(self.schema)
        return reader.read(decoder=decoder)
