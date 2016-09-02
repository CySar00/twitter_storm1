package Storm.Topologies.FindTweetsOfExperts.AfterGaussRank;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.FindingTheExpertsTweets.FindingTheExpertsTweetsBolt;
import Storm.Spouts.FindingTheExpertsTweets.FindingRankedExpertsSpout;
import Storm.Spouts.Preprocessing.OfAKeyword.ProcessTweetsOfAKeywordSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 1/9/2016.
 */
public class ExpertsTweetsOfKeyword1Topology {
    public static final String PATH_TO_RANKED_EXPERTS_FILE="/home/christina/twitter_storm_NEW/Gauss_Ranked_Experts/ranked_experts_of_keyword1.txt";
    public static final String PATH_TO_TWEETS_FILE="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword1.txt";

    public static final String PATH_TO_RANKED_EXPERTS_AND_TWEETS_FILE="/home/christina/twitter_storm_NEW/Tweets_Of_Gauss_Ranked_Experts/tweets_of_keywords1_gauss_ranked_experts.txt";


    public static void main(String[] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("PROCESS_RANKED_EXPERTS",new FindingRankedExpertsSpout(PATH_TO_RANKED_EXPERTS_FILE));
        topologyBuilder.setSpout("PROCESS_AUTHORS_AND_TWEETS_FROM_TEXT_FILE", new ProcessTweetsOfAKeywordSpout(PATH_TO_TWEETS_FILE));

        topologyBuilder.setBolt("FIND_THE_EXPERTS_TWEETS",new FindingTheExpertsTweetsBolt()).fieldsGrouping("PROCESS_AUTHORS_AND_TWEETS_FROM_TEXT_FILE",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_THE_EXPERTS_AND_TWEETS_TO_FILE",new FileWriterBolt(PATH_TO_RANKED_EXPERTS_AND_TWEETS_FILE)).shuffleGrouping("FIND_THE_EXPERTS_TWEETS");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Finding_The_Experts_Of_Keyword1_Tweets",config,topologyBuilder.createTopology());
            Utils.sleep(2*100*1000);
            localCluster.killTopology("Finding_The_Experts_Of_Keyword1_Tweets");
            localCluster.shutdown();
        }
    }
}
