package Storm.Topologies.CalculateFeatures;

import Storm.Bolts.Features.CalculateNewMetricsBolt;
import Storm.Bolts.Features.MergeFeaturesBolt;
import Storm.Bolts.FileWriterBolt;
import Storm.Spouts.Preprocessing.OfAKeyword.ProcessFeaturesOfAKeywordSpout;
import Storm.Spouts.Preprocessing.OfAKeyword.ProcessTweetsOfAKeywordSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 1/8/2016.
 */
public class FeaturesOfKeyword3Topology {
    public static final String PATH_TO_FEATURES_OF_KEYWORD3_TEXT_FILE="/home/christina/twitter_storm_NEW/Features_Seperated_Based_On_Keywords/authors_and_features_of_keyword3.txt";
    public static final String PATH_TO_TWEETS_OF_KEYWORD3_TEXT_FILE="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword3.txt";

    public static final String PATH_TO_MERGED_FEATURES_AND_NEW_METRICS_OF_KEYWORD3_TEXT_FILE="/home/christina/twitter_storm_NEW/Merged_Features_Of_Keywords/merged_features_of_keyword3.txt";

    public static void main(String[] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("PROCESS_FEATURES_OF_KEYWROD_3_FROM_TEXT_FILE",new ProcessFeaturesOfAKeywordSpout(PATH_TO_FEATURES_OF_KEYWORD3_TEXT_FILE));

        topologyBuilder.setSpout("PROCESS_TWEETS_OF_KEYWORD3_FROM_TEXT_FILE",new ProcessTweetsOfAKeywordSpout(PATH_TO_TWEETS_OF_KEYWORD3_TEXT_FILE));
        topologyBuilder.setBolt("CALCULATE_NEW_METRICS_OF_TWEETS_OF_KEYWORD3_BOLT",new CalculateNewMetricsBolt()).fieldsGrouping("PROCESS_TWEETS_OF_KEYWORD3_FROM_TEXT_FILE",new Fields("USERNAME"));
        topologyBuilder.setBolt("MERGE_NEW_METRICS_AND_FEATURES_OF_KEYWORD3_BOLT",new MergeFeaturesBolt()).fieldsGrouping("CALCULATE_NEW_METRICS_OF_TWEETS_OF_KEYWORD3_BOLT",new Fields("USERNAME"));

        topologyBuilder.setBolt("WRITE_MERGED_METRICS_AND_FEATURES_OF_KEYWROD3_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_MERGED_FEATURES_AND_NEW_METRICS_OF_KEYWORD3_TEXT_FILE)).shuffleGrouping("MERGE_NEW_METRICS_AND_FEATURES_OF_KEYWORD3_BOLT");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Merge_Features_And_New_Metrics_Of_Keyword3",config,topologyBuilder.createTopology());
            Utils.sleep(2*100*1000);
            localCluster.killTopology("Merge_Features_And_New_Metrics_Of_Keyword3");
            localCluster.shutdown();
        }
    }


}
