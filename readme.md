# Research on Types for Kafka Ingestion

This is to test an implementation of data processing within a Kafka pipeline. The Docker set up will spin up a cluster and all the monitoring components. The Python scripts will help to create the connections, maintain topics, and manage the data processing.

* [`injection.py`](./injection.py) -- this is the "main" tool. It will set up a listener for incoming messages in the configured topic in [`config.py`](./config/config.py).
* To generate events you can use [`flog`](./flog) to emit logs and pipe them to [`scream.py`](./scream.py) which will output them to the correct topic configured in [`config.py`](./config/config.py). E.g., `flog -f rfc3164 -l | python scream.py`
* To test consuming you can run [`output.py`](./output.py)

## Kafka Configuration

I used this Github as a jumping off point: https://github.com/papirosko/kafka-demo

You can see all the configuraiton settings here: [`docker-compose.yml`](./docker-compose.yaml).

## Fake Logs

I'm usng [flog](https://github.com/mingrammer/flog) to generate logs. I'm including networking in the process for the testing (since this project isn't about throughput performance beyond "whatever is reasonable").

## Does this work?

Yep! It's slow, running at around 20%-25% of realtime. See [`stats.ipynb`](./stats/stats.ipynb) for some details.