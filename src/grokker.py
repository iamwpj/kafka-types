from pygrok import Grok
from pathlib import Path
import json
from src.producer import Producer
import config.config as c
import hashlib
import re


class Grokker:
    def __init__(self) -> None:
        self.all_patterns = (
            "%{SYSLOGLINE}",
            "%{SYSLOG5424LINE}",
            ".*%{TIMESTAMP_ISO8601:timestamp}.%{SPACE}%{GREEDYDATA:message}",
        )

    def default(self, data: str) -> dict:
        result = {}

        for pattern in self.all_patterns:
            grok = Grok(pattern=pattern)
            result[pattern] = grok.match(data)

        # Return the "best" result.
        # This is just the pattern with
        # the most field matches.

        counts = [len(result[i].keys()) if result[i] else 0 for i in result]
        idx, _ = max(enumerate(counts), key=lambda x: x[1])
        return result[self.all_patterns[idx]]

    def auto_schema_gen(self, data: dict) -> str:
        # Let's build our field array
        # This seems complicated -- but it's
        # not. We only have to handle updates
        # and determining fields as strings,
        # ints, or nulls.

        fields = []

        # We don't want to handle empty keys.
        # We only add fields to the template
        # that occur here.

        rm_list = [x for x in data if not data[x]]

        for i in rm_list:
            del data[i]

        # If the logs are defined as syslog
        # They'll have a program field -- we
        # will use that to distinguish them.
        # To efficiently store the schema we will
        # hash the list of key names. This will
        # ensure we only store one shema at a time.

        name = f'{c.namespace}_{hashlib.md5("".join(sorted(list(data.keys()))).encode()).hexdigest()}'

        for i in data:
            field_name = i
            if data[i].isdigit():
                field_type = ["int"]
                # We will conver the data fields on the fly!
                data[i] = int(data[i])
            else:
                field_type = ["string"]

        if not Path(f"schemas/{name}.avsc").is_file():
            avro_template = open(f"schemas/{name}.avsc", "w+")
            fields.append({"name": field_name, "type": field_type})

            # Let's fix the timestamp field!
            if i == bool(re.search(r"^timestamp[A-z0-9].*", i)):
                fields[0]["aliases"] = ["timestamp"]

            template = {
                "namespace": c.namespace,
                "name": name,
                "type": "record",
                "fields": fields,
            }
            json.dump(template, avro_template)

            # stash it in Kafka too.
            producer = Producer()
            producer.send(
                topic=c.avro_schema_topic,
                value=json.dumps(template).encode(),
                key=name.encode(),
            )
            avro_template.close()

        return name, data
