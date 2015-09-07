#!/bin/bash

mvn clean package
storm kill jm-emitter-topology 1
sleep 20
storm jar ./target/log-kafka-storm-1.0-SNAPSHOT-jar-with-dependencies.jar com.log.kafka.mytest.emitter.EmitterTopology `docker-machine ip dev` jm-emitter-topology


