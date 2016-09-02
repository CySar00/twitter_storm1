package Storm.Topologies.SeperateTweetsAndFeaturesBasedOnKeyword;

import Storm.Bolts.FileWriterBolt;
import Storm.Spouts.Preprocessing.FromCSVFiles.ProcessFeaturesOfKeywordSpout;
import Storm.Spouts.Preprocessing.FromCSVFiles.ProcessTweetsOfKeywordsSpout;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 29/7/2016.
 */
public class SeperateFeaturesAndTweetsBasedOnKeywordsTopology {
    public static final String PATH_TO_FEATURES_CSV_FILE="/home/christina/twitter_storm_NEW/CSV/features.csv";
    public static final String PATH_TO_TWEETS_CSV_FILE="/home/christina/twitter_storm_NEW/CSV/tweets.csv";

    public static final String PATH_TO_FEATURES_OF_KEYWORD1_TEXT_FIlE="/home/christina/twitter_storm_NEW/Features_Seperated_Based_On_Keywords/authors_and_features_of_keyword1.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD2_TEXT_FILE="/home/christina/twitter_storm_NEW/Features_Seperated_Based_On_Keywords/authors_and_features_of_keyword2.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD3_TEXT_FILE="/home/christina/twitter_storm_NEW/Features_Seperated_Based_On_Keywords/authors_and_features_of_keyword3.txt";

    public static final String PATH_TO_TWEETS_OF_KEYWORD1_TEXT_FILE="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword1.txt";
    public static final String PATH_TO_TWEETS_OF_KEYWORD2_TEXT_FILE="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword2.txt";
    public static final String PATH_TO_TWEETS_OF_KEYWORD3_TEXT_FILE="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword3.txt";

    public static final String keyword1="#blacklivesmatter";
    public static final String keyword2="#germanwings";
    public static final String keyword3="#bigdata";

    public static void main(String[] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("FIND_FEATURES_OF_KEYWORD1_FROM_FEATURES_CSV_FILE",new ProcessFeaturesOfKeywordSpout(PATH_TO_FEATURES_CSV_FILE,keyword1));
        topologyBuilder.setSpout("FIND_FEATURES_OF_KEYWORD2_FROM_FEATURES_CSV_FILE",new ProcessFeaturesOfKeywordSpout(PATH_TO_FEATURES_CSV_FILE,keyword2));
        topologyBuilder.setSpout("FIND_FEATURES_OF_KEYWORD3_FROM_FEATURES_CSV_FILE",new ProcessFeaturesOfKeywordSpout(PATH_TO_FEATURES_CSV_FILE,keyword3));


        topologyBuilder.setSpout("FIND_TWEETS_OF_KEYWORD1_FROM_TWEETS_CSV_FILE",new ProcessTweetsOfKeywordsSpout(PATH_TO_TWEETS_CSV_FILE,keyword1));
        topologyBuilder.setSpout("FIND_TWEETS_OF_KEYWORD2_FROM_TWEETS_CSV_FILE",new ProcessTweetsOfKeywordsSpout(PATH_TO_TWEETS_CSV_FILE,keyword2));
        topologyBuilder.setSpout("FIND_TWEETS_OF_KEYWORD3_FROM_TWEETS_CSV_FILE",new ProcessTweetsOfKeywordsSpout(PATH_TO_TWEETS_CSV_FILE,keyword3));

        topologyBuilder.setBolt("WRITE_FEATURES_OF_KEYWORD1_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD1_TEXT_FIlE)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD1_FROM_FEATURES_CSV_FILE");
        topologyBuilder.setBolt("WRITE_FEATURES_OF_KEYWORD2_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD2_TEXT_FILE)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD2_FROM_FEATURES_CSV_FILE");
        topologyBuilder.setBolt("WRITE_FEATURES_OF_KEYWORD3_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_FEATURES_OF_KEYWORD3_TEXT_FILE)).shuffleGrouping("FIND_FEATURES_OF_KEYWORD3_FROM_FEATURES_CSV_FILE");

        topologyBuilder.setBolt("WRITE_TWEETS_OF_KEYWORD1_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_TWEETS_OF_KEYWORD1_TEXT_FILE)).shuffleGrouping("FIND_TWEETS_OF_KEYWORD1_FROM_TWEETS_CSV_FILE");
        topologyBuilder.setBolt("WRITE_TWEETS_OF_KEYWORD2_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_TWEETS_OF_KEYWORD2_TEXT_FILE)).shuffleGrouping("FIND_TWEETS_OF_KEYWORD2_FROM_TWEETS_CSV_FILE");
        topologyBuilder.setBolt("WRITE_TWEETS_OF_KEYWORD3_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_TWEETS_OF_KEYWORD3_TEXT_FILE)).shuffleGrouping("FIND_TWEETS_OF_KEYWORD3_FROM_TWEETS_CSV_FILE");


        backtype.storm.Config config=new backtype.storm.Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Seperate_Features_And_Tweets_Based_On_Keywords",config,topologyBuilder.createTopology());
            Utils.sleep(2*100*1000);
            localCluster.killTopology("Seperate_Features_And_Tweets_Based_On_Keywords");
            localCluster.shutdown();
        }



    }
}
