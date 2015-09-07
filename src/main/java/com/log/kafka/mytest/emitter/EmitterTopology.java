package com.log.kafka.mytest.emitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.log.kafka.storm.common.EWMA;
import com.log.kafka.storm.common.NotifyMessageMapper;
import com.log.kafka.storm.filter.BooleanFilter;
import com.log.kafka.storm.function.JsonProjectFunction;
import com.log.kafka.storm.function.MovingAverageFunction;
import com.log.kafka.storm.function.ThresholdFilterFunction;
import com.log.kafka.storm.function.XMPPFunction;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;

import java.util.Arrays;


public class EmitterTopology {


  public StormTopology buildTopology() {

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("words", new TestWordSpout(), 10);
    builder.setBolt("exclaim1", new KafkaBolt(), 3)
            .shuffleGrouping("words");
    return builder.createTopology();
  }

  public static void main(String[] args) throws Exception {

    String dockerIp = args[0];

    Config conf = new Config();
    conf.setMaxSpoutPending(5);

    if (args.length > 1) {
      EmitterTopology mytopology = new EmitterTopology();
      conf.setNumWorkers(3);
      conf.put(Config.NIMBUS_HOST, dockerIp);
      conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
      conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
      conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
      StormSubmitter.submitTopology(args[1], conf, mytopology.buildTopology());
    } else {
      EmitterTopology mytopology = new EmitterTopology();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("testSpout", conf, mytopology.buildTopology());
    }
  }
}