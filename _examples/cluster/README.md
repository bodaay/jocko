# Quafka cluster example

This will start a local three node cluster.

## Build

```bash
git clone https://github.com/bodaay/quafka.git
cd quafka
make build
```

## Start the nodes

```bash
./cmd/quafka/quafka broker \
          --data-dir="/tmp/quafka0" \
          --broker-addr=127.0.0.1:9001 \
          --raft-addr=127.0.0.1:9002 \
          --serf-addr=127.0.0.1:9003 \
          --bootstrap \
          --bootstrap-expect=3 \
          --id=1

./cmd/quafka/quafka broker \
          --data-dir="/tmp/quafka1" \
          --broker-addr=127.0.0.1:9101 \
          --raft-addr=127.0.0.1:9102 \
          --serf-addr=127.0.0.1:9103 \
          --join=127.0.0.1:9003 \
          --bootstrap-expect=3 \
          --id=2

./cmd/quafka/quafka broker \
          --data-dir="/tmp/quafka2" \
          --broker-addr=127.0.0.1:9201 \
          --raft-addr=127.0.0.1:9202 \
          --serf-addr=127.0.0.1:9203 \
          --join=127.0.0.1:9003 \
          --bootstrap-expect=3 \
          --id=3
```

## docker-compose cluster

To start a [docker compose](https://docs.docker.com/compose/) cluster use the provided `docker-compose.yml`.
