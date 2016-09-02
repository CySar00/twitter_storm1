package Storm.Topologies.RankResults.FindingTheExperts;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.FindTheExpertsBolt;
import Storm.Spouts.ProcessMergedFeaturesOfAKeywordSpout;
import Storm.Spouts.Rank.ProcessingTheExpertsSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 12/8/2016.
 */
public class FindingTheExpertsOfKeyword1Topology {
    public static final String PATH_TO_EXPERTS_FILE="/home/christina/twitter_storm_NEW/Checked_Gaussian_Probabilities/gaussian_probabilities_of_features_of_cluster_1_and_keyword1.txt";

    public static final String PATH_TO_MERGED_FEATURES_OF_KEYWORD="/home/christina/twitter_storm_NEW/Merged_Features_Of_Keywords/merged_features_of_keyword1.txt";
    public static final String PATH_TO_EXPERTS_OF_KEYWORD="/home/christina/twitter_storm_NEW/ExpertsOfKeywords/ExpertsOfKeyword1.txt";


    public static void main(String[] args) throws AlreadyAliveException,Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("PROCESSING_THE_EXPERTS_OF_KEYWORD",new ProcessingTheExpertsSpout(PATH_TO_EXPERTS_FILE));

        topologyBuilder.setSpout("PROCESS_MERGED_FEATURES_OF_KEYWORD_FROM_TEXT_FILE",new ProcessMergedFeaturesOfAKeywordSpout(PATH_TO_MERGED_FEATURES_OF_KEYWORD));
        topologyBuilder.setBolt("FIND_EXPERTS_OF_KEYWORD",new FindTheExpertsBolt()).shuffleGrouping("PROCESS_MERGED_FEATURES_OF_KEYWORD_FROM_TEXT_FILE");
        topologyBuilder.setBolt("WRITE_EXPERTS_OF_KEYWORD_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_EXPERTS_OF_KEYWORD)).shuffleGrouping("FIND_EXPERTS_OF_KEYWORD");



        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Finding_The_Experts_Of_Keyword1", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Finding_The_Experts_Of_Keyword1", config, topologyBuilder.createTopology());
            Utils.sleep(2*60*1000);
            localCluster.killTopology("Finding_The_Experts_Of_Keyword1");
            localCluster.shutdown();
        }
    }
}
