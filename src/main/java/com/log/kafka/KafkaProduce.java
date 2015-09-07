package com.log.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * Created by ajmssc on 9/6/15.
 */
public class KafkaProduce {

    public static void main(String[] args) {
        long events = 100L;
        Random rnd = new Random();
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.99.101:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("retry.backoff.ms", "150");
        props.put("partitioner.class", "com.log.kafka.SimplePartitioner");
        //props.put("request.required.acks", "0");
        props.put("topic.metadata.refresh.interval.ms", "0");


        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < events; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + runtime + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
            try {
                producer.send(data);
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        producer.close();

    }



}

