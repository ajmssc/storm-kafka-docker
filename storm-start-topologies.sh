#!/bin/bash

mvn clean package
storm kill jm-emitter-topology 1
sleep 20
storm jar ./target/log-kafka-storm-1.0-SNAPSHOT-jar-with-dependencies.jar com.log.kafka.mytest.emitter.EmitterTopology 192.168.99.101 jm-emitter-topology


