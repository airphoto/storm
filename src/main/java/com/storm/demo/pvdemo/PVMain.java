package com.storm.demo.pvdemo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by abel on 16-8-27.
 */
public class PVMain {
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("pv",new PVSpout(),1);
        builder.setBolt("p1",new ThreadSumBolt(),2).shuffleGrouping("pv");
        builder.setBolt("p2",new SumBolt(),1).shuffleGrouping("p1");

        Config config = new Config();
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("pvTest",config,builder.createTopology());
    }
}
