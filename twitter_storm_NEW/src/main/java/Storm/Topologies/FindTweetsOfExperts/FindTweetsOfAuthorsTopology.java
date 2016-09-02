package Storm.Topologies.FindTweetsOfExperts;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.FindingTheExpertsTweets.FindingTheExpertsTweetsBolt;
import Storm.Bolts.TwitterInfo.GatherAuthorsAndTweetsBolt;
import Storm.Spouts.FindingTheExpertsTweets.FindingRankedExpertsSpout;
import Storm.Spouts.Preprocessing.OfAKeyword.ProcessTweetsOfAKeywordSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 2/9/2016.
 */
public class FindTweetsOfAuthorsTopology {
    public static final String PATH_TO_TWEETS_OF_KEYWORD1="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword1.txt";
    public static final String PATH_TO_TWEETS_OF_KEYWORD2="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword2.txt";
    public static final String PATH_TO_TWEETS_OF_KEYWORD3="/home/christina/twitter_storm_NEW/Tweets_Seperated_Based_On_Keywords/authors_and_tweets_of_keyword3.txt";

    public static final String PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD1="/home/christina/twitter_storm_NEW/Tweets_Of_Authors/authors_tweets_of_keyword1.txt";
    public static final String PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD2="/home/christina/twitter_storm_NEW/Tweets_Of_Authors/authors_tweets_of_keyword2.txt";
    public static final String PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD3="/home/christina/twitter_storm_NEW/Tweets_Of_Authors/authors_tweets_of_keyword3.txt";

    public static void main(String[] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("PROCESS_AUTHORS_AND_TWEETS_OF_KEYWORD1",new ProcessTweetsOfAKeywordSpout(PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD1));
        topologyBuilder.setBolt("FIND_AUTHORS_TWEETS_OF_KEYWORD1",new GatherAuthorsAndTweetsBolt()).fieldsGrouping("PROCESS_AUTHORS_AND_TWEETS_OF_KEYWORD1",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_AUTHORS_AND_TWEETS_OF_KEYWORD1_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD1)).shuffleGrouping("FIND_AUTHORS_TWEETS_OF_KEYWORD1");


        topologyBuilder.setSpout("PROCESS_AUTHORS_AND_TWEETS_OF_KEYWORD2",new ProcessTweetsOfAKeywordSpout(PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD2));
        topologyBuilder.setBolt("FIND_AUTHORS_TWEETS_OF_KEYWORD2",new GatherAuthorsAndTweetsBolt()).fieldsGrouping("PROCESS_AUTHORS_AND_TWEETS_OF_KEYWORD2",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_AUTHORS_AND_TWEETS_OF_KEYWORD2_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD2)).shuffleGrouping("FIND_AUTHORS_TWEETS_OF_KEYWORD2");

        topologyBuilder.setSpout("PROCESS_AUTHORS_AND_TWEETS_OF_KEYWORD3",new ProcessTweetsOfAKeywordSpout(PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD3));
        topologyBuilder.setBolt("FIND_AUTHORS_TWEETS_OF_KEYWORD3",new GatherAuthorsAndTweetsBolt()).fieldsGrouping("PROCESS_AUTHORS_AND_TWEETS_OF_KEYWORD3",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_AUTHORS_AND_TWEETS_OF_KEYWORD3_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_AUTHOR_AND_TWEETS_OF_KEYWORD1)).shuffleGrouping("FIND_AUTHORS_TWEETS_OF_KEYWORD3");



        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(10);
            config.setNumAckers(5);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
        }else{
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology("Finding_The_Authors_Tweets",config,topologyBuilder.createTopology());
            Utils.sleep(2*100*1000);
            localCluster.killTopology("Finding_The_Authors_Tweets");
            localCluster.shutdown();
        }
    }
}
