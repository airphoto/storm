package com.storm.demo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class WordCountSumBolt extends BaseRichBolt {

    private Map<String,Integer> map = null;
    private String word = null;
    private OutputCollector outputCollector = null;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = new HashMap<String,Integer>();
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        this.word = tuple.getStringByField("word");
        if(map.containsKey(word)){
            map.put(word,map.get(word)+1);
        }else{
            map.put(word,1);
        }

        for(Map.Entry<String,Integer> entry : map.entrySet()){
            outputCollector.emit(new Values(entry.getKey(),entry.getValue()));
            System.err.println(Thread.currentThread().getName()+"  "+entry.getKey()+"-->"+entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key","value"));
    }
}
