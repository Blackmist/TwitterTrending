package com.microsoft.example;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FirstN;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.StormSubmitter;

import java.io.IOException;

import com.microsoft.example.HashtagExtractor;
import com.microsoft.example.TwitterSpout;

public class TwitterTrendingTopology {
  //Build the topology
  public static StormTopology buildTopology(IBatchSpout spout) throws IOException {
    final TridentTopology topology = new TridentTopology();
    //Define the topology:
    //1. spout reads tweets
    //2. HashtagExtractor emits hashtags pulled from tweets
    //3. hashtags are grouped
    //4. a count of each hashtag is created
    //5. each hashtag, and how many times it has occured
    //   is emitted.
    topology.newStream("spout", spout)
    .each(new Fields("tweet"), new HashtagExtractor(), new Fields("hashtag"))
    .groupBy(new Fields("hashtag"))
    .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
    .newValuesStream()
    .applyAssembly(new FirstN(10, "count"))
    .each(new Fields("hashtag", "count"), new Debug());
    //Build and return the topology
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    final Config conf = new Config();
    final IBatchSpout spout = new TwitterSpout();
    //If no args, assume we are testing locally
    if(args.length==0) {
      //create LocalCluster and submit
      final LocalCluster local = new LocalCluster();
      try {
        local.submitTopology("hashtag-count-topology", conf, buildTopology(spout));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      //If on a real cluster, set the workers and submit
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(spout));
    }
  }
}
