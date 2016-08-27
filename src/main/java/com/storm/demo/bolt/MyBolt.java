package com.storm.demo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by abel on 16-8-26.
 */
public class MyBolt implements IRichBolt{

    OutputCollector outputCollector = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    int num = 0;
    String value = null;
    public void execute(Tuple tuple) {
        try{
            value = tuple.getStringByField("log");
            if(value != null){
                num ++;
                System.err.println("lines : "+num + "   -->"+value);
                outputCollector.ack(tuple);
            }
        }catch (Exception e){
            outputCollector.fail(tuple);
            e.printStackTrace();
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare(new Fields(""));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
