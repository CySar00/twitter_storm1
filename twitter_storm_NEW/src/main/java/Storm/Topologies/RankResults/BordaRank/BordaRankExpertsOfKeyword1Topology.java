package Storm.Topologies.RankResults.BordaRank;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.BordaRankTheExpertsBolt;
import Storm.Spouts.Rank.Borda.ProcesssTheExpertsAndTheirFeaturesSpout1;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 1/9/2016.
 */
public class BordaRankExpertsOfKeyword1Topology {
    public static final String PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE1="/home/christina/twitter_storm_NEW/ExpertsOfKeywords/ExpertsOfKeyword1AndFuzzyCluster0.txt";
    public static final String PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE2="/home/christina/twitter_storm_NEW/ExpertsOfKeywords/ExpertsOfKeyword1AndFuzzyCluster1.txt";

    public static final String RANKED_EXPERTS_OF_KEYWORD1="/home/christina/twitter_storm_NEW/Borda_Ranked_Experts/Ranked_Experts_Of_Keyword1_And_Fuzzy_Cluster0.txt";
    public static final String RANKED_EXPERTS_OF_KEYWORD2="/home/christina/twitter_storm_NEW/Borda_Ranked_Experts/Ranked_Experts_Of_Keyword1_And_Fuzzy_Cluster1.txt";

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE1",new ProcesssTheExpertsAndTheirFeaturesSpout1(PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE1));
        topologyBuilder.setBolt("BORDA_RANK_THE_EXPERTS_OF_KEYWORD1",new BordaRankTheExpertsBolt()).shuffleGrouping("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE1");
        topologyBuilder.setBolt("WRITE_RANKED_EXPERTS_OF_KEYWORD_TO_TEXT_FILE1",new FileWriterBolt(RANKED_EXPERTS_OF_KEYWORD1)).shuffleGrouping("BORDA_RANK_THE_EXPERTS_OF_KEYWORD1");

        topologyBuilder.setSpout("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE2",new ProcesssTheExpertsAndTheirFeaturesSpout1(PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE2));
        topologyBuilder.setBolt("BORDA_RANK_THE_EXPERTS_OF_KEYWORD2",new BordaRankTheExpertsBolt()).shuffleGrouping("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE2");
        topologyBuilder.setBolt("WRITE_RANKED_EXPERTS_OF_KEYWORD_TO_TEXT_FILE2",new FileWriterBolt(RANKED_EXPERTS_OF_KEYWORD2)).shuffleGrouping("BORDA_RANK_THE_EXPERTS_OF_KEYWORD2");




        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Borda_Rank_The_Experts_Of_Keyword1", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Borda_Rank_The_Experts_Of_Keyword1", config, topologyBuilder.createTopology());
            Utils.sleep(20 * 1000);
            localCluster.killTopology("Borda_Rank_The_Experts_Of_Keyword1");
            localCluster.shutdown();
        }
    }
}
