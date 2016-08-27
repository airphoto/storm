package com.storm.demo.pvdemo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class PVSpout implements IRichSpout {

    private InputStream is ;
    private InputStreamReader isr;
    private BufferedReader br;
    private SpoutOutputCollector spoutOutputCollector;
    private String str = null;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.is = new FileInputStream("/home/abel/IdeaProjects/storm/demo/src/main/resources/log");
            this.isr = new InputStreamReader(is);
            this.br = new BufferedReader(isr);
            this.spoutOutputCollector = spoutOutputCollector;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        try {
            while ((str = br.readLine()) != null){
                spoutOutputCollector.emit(new Values(str));
                Thread.sleep(100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }


    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

}
