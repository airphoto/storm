package com.storm.demo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class WordCountDistinctBolt extends BaseRichBolt {
    private Map<String,Integer> map = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
//        int key_sum = 0;
        int value_sum = 0;
        String key = tuple.getStringByField("key");
        Integer value = tuple.getIntegerByField("value");
        map.put(key,value);


        Iterator<Integer> iterator = map.values().iterator();
        while (iterator.hasNext()){
          value_sum+=iterator.next();
        }

        System.err.println("key size -->"+map.entrySet().size() + ";  values-->" +value_sum);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
