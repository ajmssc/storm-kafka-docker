package com.log.kafka.mytest.emitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

/**
 * Created by ajmssc on 9/6/15.
 */


public class KafkaBolt implements IRichBolt {
    OutputCollector _collector;
    Producer<String, String> kafka;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

        Properties props = new Properties();
        props.put("metadata.broker.list", "stormkafkadocker_kafka1_1:9092,stormkafkadocker_kafka2_1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("producer.type","async");
//        props.put("retry.backoff.ms", "150");
//        props.put("partitioner.class", "com.log.kafka.SimplePartitioner");
//        //props.put("request.required.acks", "0");
//        props.put("topic.metadata.refresh.interval.ms", "0");
        ProducerConfig config = new ProducerConfig(props);

        kafka = new Producer<String, String>(config);
    }

    public void execute(Tuple tuple) {
        String message = tuple.getString(0);
        _collector.emit(tuple, new Values(message));
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("storm_input", "0" + message.hashCode(), message);
        kafka.send(data);
        _collector.ack(tuple);
    }

    public void cleanup() {
        kafka.close();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}