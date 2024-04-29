# Schemas

These are Apache Avro schemes. The code in [`schemas.py`](../src/schemas.py) will define files here for use. During runtime these are stored in a running Apache Kafka and pulled for decode.

* TODO: Technically I don't need local storage at all, the encode could happen from the centralized storage. This is a small code rewrite process as it should sort out some of the `src` file rewrites.