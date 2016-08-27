package com.storm.demo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class WordCountSplitBolt extends BaseRichBolt {

    private String str = null;
    private OutputCollector outputCollector;
    private String pattern = " ";
    private Fields fields = new Fields("word");

    public  WordCountSplitBolt(){

    }

    public WordCountSplitBolt(String pattern){
        this.pattern = pattern;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        str = tuple.getStringByField("words");
        for(String word : str.split(pattern)){
            outputCollector.emit(new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(fields);
    }
}
