package Storm.Topologies.RankResults.GaussianRank;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.CalculateCDFsOfExpertsFeaturesBolt;
import Storm.Spouts.Rank.ProcessTheExpertsAndTheirFeaturesSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 12/8/2016.
 */
public class CalculateTheCDFsOfFeaturesOfKeyword2Topology {
    public static final String PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE="/home/christina/twitter_storm_NEW/ExpertsOfKeywords/ExpertsOfKeyword2.txt";
    public static final String PATH_TO_CDFS_OF_AUTHORS="/home/christina/twitter_storm_NEW/CDFs_Of_Features/experts_and_cdfs_of_features_of_keyword2.txt";


    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE",new ProcessTheExpertsAndTheirFeaturesSpout(PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE));

        topologyBuilder.setBolt("CALCULATE_CDFS_OF_FEATURES",new CalculateCDFsOfExpertsFeaturesBolt()).fieldsGrouping("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE",new Fields("USERNAME"));
        topologyBuilder.setBolt("WRITE_CDFS_OF__FEATURES_TO_TXT_FILE",new FileWriterBolt(PATH_TO_CDFS_OF_AUTHORS)).shuffleGrouping("CALCULATE_CDFS_OF_FEATURES");


        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Calculating_The_CDFs_Of_The_Experts_Of_Keyword2", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Calculating_The_CDFs_Of_The_Experts_Of_Keyword2", config, topologyBuilder.createTopology());
            Utils.sleep(20 * 1000);
            localCluster.killTopology("Calculating_The_CDFs_Of_The_Experts_Of_Keyword2");
            localCluster.shutdown();
        }
    }
}
