package com.storm.demo.pvdemo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class ThreadSumBolt implements IRichBolt {

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    private String str;
    public void execute(Tuple tuple) {
        this.str = tuple.getString(0);
        if(str != null && !str.trim().equals("")){
            System.err.println("thread-->"+Thread.currentThread().getId()+"  ;   sum->"+1);
            outputCollector.emit(new Values(Thread.currentThread().getId(),1L));
        }

    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("threadId","sum"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
