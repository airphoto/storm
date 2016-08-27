package com.storm.demo.main;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.storm.demo.bolt.WordCountDistinctBolt;
import com.storm.demo.bolt.WordCountSplitBolt;
import com.storm.demo.bolt.WordCountSumBolt;
import com.storm.demo.spout.WordCountSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by abel on 16-8-27.
 */
public class WordCountTopology {
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("producer",new WordCountSpout(),1);
        builder.setBolt("split",new WordCountSplitBolt(),5).shuffleGrouping("producer");
        builder.setBolt("sum",new WordCountSumBolt(),5).fieldsGrouping("split",new Fields("word"));
        builder.setBolt("distinct",new WordCountDistinctBolt(),1).shuffleGrouping("sum");

        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS,4);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordcounts",conf,builder.createTopology());
    }
}
