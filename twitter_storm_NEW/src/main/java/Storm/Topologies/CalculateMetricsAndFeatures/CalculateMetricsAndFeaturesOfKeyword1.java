package Storm.Topologies.CalculateMetricsAndFeatures;

import Storm.Bolts.MetricsAndFeatures.CalculateMetricsAndFeaturesBolt;
import Storm.Spouts.Preprocessing.OfAKeyword.ProcessTweetsOfAKeywordSpout;
import Storm.Spouts.Preprocessing.ProcessTwitterInfoFromTextAndCSVFilesSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 1/8/2016.
 */
public class CalculateMetricsAndFeaturesOfKeyword1 {
    public static final String PATH_TO_TWEETS_OF_KEYWORD1_TEXT_FILE="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword1.txt";

    public static final String PATH_TO_TWEETS_CSV_FILE="/home/christina/twitter_storm_NEW/CSV/tweets.csv";
    public static final String PATH_TO_USER_IDS_CSV_FILE="/home/christina/twitter_storm_NEW/CSV/ids.csv";
    public static final String PATH_TO_FOLLOWERS_CSV_FILE="/home/christina/twitter_storm_NEW/CSV/followers.csv";
    public static final String PATH_TO_FREINDS_CSV_FILE="/home/christina/twitter_storm_NEW/CSV/friends.csv";

    public static final String keyword1="#blacklivesmatter";

    public static void main(String [] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("PROCESS_TWITTER_INFORMATION_FROM_TEXT_AND_CSV_FILES",new ProcessTwitterInfoFromTextAndCSVFilesSpout(PATH_TO_TWEETS_OF_KEYWORD1_TEXT_FILE,PATH_TO_USER_IDS_CSV_FILE,PATH_TO_FOLLOWERS_CSV_FILE,PATH_TO_FREINDS_CSV_FILE));

        topologyBuilder.setSpout("PROCESS_TWEETS_OF_KEYWORD1_FROM_TEXT_FILE",new ProcessTweetsOfAKeywordSpout(PATH_TO_TWEETS_OF_KEYWORD1_TEXT_FILE));


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Calculate_Metrics_And_Features_Of_Keyword1",config,topologyBuilder.createTopology());
            Utils.sleep(2*100*1000);
            localCluster.killTopology("Calculate_Metrics_And_Features_Of_Keyword1");
            localCluster.shutdown();

        }
    }
}
