package com.storm.demo.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * Created by abel on 16-8-26.
 */
public class MySpout implements IRichSpout {

    SpoutOutputCollector spoutOutputCollector = null;
    FileInputStream fis ;
    InputStreamReader isr;
    BufferedReader br;

    /**
     * 初始化方法，只执行一次
     * @param map                     配置
     * @param topologyContext
     * @param spoutOutputCollector      发射器
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        try {
            this.fis = new FileInputStream("/home/abel/examples.desktop");
            this.isr = new InputStreamReader(fis,"UTF-8");
            this.br = new BufferedReader(isr);
            this.spoutOutputCollector = spoutOutputCollector;
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    String str = null;
    public void nextTuple() {
        try {
            while ((str = this.br.readLine()) !=null){
                //values 是List的子类
                spoutOutputCollector.emit(new Values(str));
                Thread.sleep(30);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //Fields
        outputFieldsDeclarer.declare(new Fields("log"));
    }



    public void close() {
        try {
            fis.close();
            isr.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void activate() {

    }

    public void deactivate() {

    }


    public void ack(Object o) {
        System.err.println("ok-->"+o);
    }

    public void fail(Object o) {
        System.err.println("not ok-->"+o);
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
