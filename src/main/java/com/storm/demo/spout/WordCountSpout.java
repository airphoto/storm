package com.storm.demo.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class WordCountSpout extends BaseRichSpout {

    private String[] str = new String[]{"a b c","a b c","a b c"};
    private SpoutOutputCollector spoutOutputCollector;

    private Fields fields = new Fields("words");

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }


    public void nextTuple() {
        while (true) {
            for (String words : str) {
                spoutOutputCollector.emit(new Values(words));
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(fields);
    }
}
