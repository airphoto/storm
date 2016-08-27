package com.storm.demo.main;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.storm.demo.bolt.MyBolt;
import com.storm.demo.spout.MySpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by abel on 16-8-26.
 */
public class MainTopology {

    public static void main(String[] args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spot",new MySpout(),1);
        topologyBuilder.setBolt("bolt",new MyBolt(),1).shuffleGrouping("spot");//数据源来自spot

        Map conf = new HashMap();
        conf.put(Config.TOPOLOGY_WORKERS,4);

        if(args.length==0) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("topology", conf, topologyBuilder.createTopology());
//            localCluster.shutdown();
        }else{
            try {
                StormSubmitter.submitTopology("topology", conf, topologyBuilder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }
    }

}
