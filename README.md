# Kafka Cron Job Scheduler

A Java application that reads cron-style schedules and publishes jobs to Apache Kafka. Kafka consumers execute the configured shell commands, route jobs by cluster, and send failed jobs to a retry topic until their configured attempt limit is reached.

The included Docker Compose environment runs Kafka, a Conduktor Console, one scheduler producer, and main and retry consumers for two example clusters.

## Features

- Read cron schedules from a text file and trigger jobs with Quartz.
- Route scheduled jobs to a Kafka topic for the configured cluster.
- Execute commands in a shell and log their output and exit status.
- Retry commands that exit unsuccessfully, up to the configured attempt count.
- Run locally with Maven or launch the full example stack with Docker Compose.

## Prerequisites

- **Java 17 or later** and **Maven 3.9 or later** to build and run the Java application directly.
- **Docker Engine** with the **Docker Compose plugin** to run the full example stack.
- A Kafka broker accessible to the producer and consumers when running the Java application outside Docker.

## Getting started with Docker Compose

From the project root, start the example environment:

```sh
docker compose -f conduktor-kafka-single.yml up --build -d
```

Or, if `make` is installed:

```sh
make up
```

The stack builds the Java services, starts Kafka, creates the `cron-jobs` topic, and starts the producer and four consumers. The producer reads `crontab.txt` from the container and emits jobs at their scheduled times.

- **Conduktor Console:** <http://localhost:8080>
- **Kafka external listener:** `localhost:9092`

View service logs:

```sh
docker compose -f conduktor-kafka-single.yml logs -f producer consumer-cluster-a-main consumer-cluster-a-retry consumer-cluster-b-main consumer-cluster-b-retry
```

Or use `make logs`. Stop the services with:

```sh
docker compose -f conduktor-kafka-single.yml down
```

`make down` also removes the Compose volumes (`-v`), including persisted Kafka Console and database data.

## Schedule jobs

Edit `crontab.txt` to add or change jobs. Each non-comment line has this format:

```text
minute hour day-of-month month day-of-week cluster max-attempts command
```

For example:

```text
* * * * * cluster-a 3 echo "Always succeed"
* * * * * cluster-b 4 sh -c 'if [ $((RANDOM % 2)) -eq 0 ]; then echo "Success!"; exit 0; else echo "Flaky failure"; exit 1; fi'
15 * * * * cluster-a 3 exit 1
```

The schedule uses the standard five-field cron format. Blank lines and lines beginning with `#` are ignored. The cluster name determines the main topic (`cron-jobs-<cluster>`); for example, `cluster-a` routes to `cron-jobs-cluster-a`. A failed command is sent to the corresponding retry topic (for example, `cron-jobs-cluster-a-retry`) while attempts remain. The attempt count includes the initial run. Only commands that finish with a non-zero exit status are retried; a command that times out is stopped without being retried.

Commands are run by `/bin/sh -c` inside the consumer container. Only add commands you trust: a schedule can run arbitrary shell commands with the consumer container's permissions. The sample commands are demonstrations; replace them with the commands appropriate for your environment.

The producer reads the schedule when it starts. Restart it after editing the file to load changes:

```sh
docker compose -f conduktor-kafka-single.yml restart producer
```

## Run the application with Maven

Build the executable jar:

```sh
mvn package
```

The scheduler is the jar's default entry point. Set the Kafka bootstrap address and pass the schedule file as an optional argument (it defaults to `crontab.txt`):

```sh
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
  java -jar target/kafka-cron-job-scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar crontab.txt
```

Run a consumer in another terminal. Set its topic to the desired cluster's main topic; its retry topic is derived by adding `-retry`:

```sh
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_TOPIC=cron-jobs-cluster-a \
CONSUMER_ID=Cluster-A-Worker \
java -cp target/kafka-cron-job-scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar \
  com.github.eediallo.scheduler.consumer.Main
```

To consume jobs from another cluster, set `KAFKA_TOPIC` to its `cron-jobs-<cluster>` topic. The producer and consumers must be able to connect to the same Kafka broker.

## Configuration

| Environment variable | Used by | Default | Description |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Producer and consumer | Producer: `localhost:19092`; consumer: `localhost:9092` | Kafka bootstrap server address. Set explicitly when running locally. |
| `KAFKA_TOPIC` | Producer and consumer | Consumer: `cron-jobs` | Consumer topic. Scheduled jobs are routed to `cron-jobs-<cluster>` based on each schedule line. |
| `CONSUMER_ID` | Consumer | `Consumer-Default` | Identifier included in consumer log messages. |
| `CONSUMER_GROUP` | Consumer | `cron-execution-group` | Kafka consumer group ID. |

The Compose setup supplies these values for its services. Its Kafka broker and Conduktor settings are configured in `conduktor-kafka-single.yml`.

## Tests

Run the unit tests with:

```sh
mvn test
```
