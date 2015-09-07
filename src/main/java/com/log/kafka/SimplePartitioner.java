package com.log.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
* Created by ajmssc on 9/6/15.
*/
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner(VerifiableProperties props) {

    }

    public int partition(Object key, int a_numPartitions) {

        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = stringKey.hashCode() % a_numPartitions;
            //partition = Integer.parseInt(stringKey.substring(offset + 1)) % a_numPartitions;
        }
        //return 0;
        return partition;
    }
}
