#!/bin/bash

docker exec stormkafkadocker_kafka2_1 bash /opt/kafka_2.10-0.8.2.0/bin/kafka-topics.sh --zookeeper stormkafkadocker_zookeeper_1 --topic storm_input --create --replica-assignment 1:2,2:1
