package com.storm.demo.pvdemo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class SumBolt extends BaseRichBolt {

    private Map<Long,Long> map;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = new HashMap<Long, Long>();
    }

    public void execute(Tuple tuple) {
        Long threadId = tuple.getLongByField("threadId");
        Long sum = tuple.getLongByField("sum");
        if(map.containsKey(threadId)){
            map.put(threadId,map.get(threadId)+sum);
        }else{
            map.put(threadId,sum);
        }

        Long all = 0L;

        for(Map.Entry<Long,Long> entry:map.entrySet()){
            all+=entry.getValue();
        }

        System.err.println("pv = "+ all);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
