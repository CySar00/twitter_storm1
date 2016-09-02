package Storm.Topologies.RankResults.BordaRank;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.BordaRankTheExpertsBolt;
import Storm.Bolts.Rank.CalculateCDFsOfExpertsFeaturesBolt;
import Storm.Spouts.Rank.Borda.ProcesssTheExpertsAndTheirFeaturesSpout1;
import Storm.Spouts.Rank.ProcessTheExpertsAndTheirFeaturesSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 31/8/2016.
 */
public class BordaRankExpertsOfKeyword3Topology {
    public static final String PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE="/home/christina/twitter_storm_NEW/ExpertsOfKeywords/ExpertsOfKeyword3AfterFuzzyClustering.txt";
    public static final String RANKED_EXPERTS_OF_KEYWORD="/home/christina/twitter_storm_NEW/Borda_Ranked_Experts/Ranked_Experts_Of_Keyword3.txt";


    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE",new ProcesssTheExpertsAndTheirFeaturesSpout1(PATH_TO_EXPERTS_OF_KEYWORD_TEXT_FILE));
        topologyBuilder.setBolt("BORDA_RANK_THE_EXPERTS_OF_KEYWORD",new BordaRankTheExpertsBolt()).shuffleGrouping("READ_EXPERTS_AND_FEATURES_FROM_TEXT_FILE");
        topologyBuilder.setBolt("WRITE_RANKED_EXPERTS_OF_KEYWORD_TO_TEXT_FILE",new FileWriterBolt(RANKED_EXPERTS_OF_KEYWORD)).shuffleGrouping("BORDA_RANK_THE_EXPERTS_OF_KEYWORD");


        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Borda_Rank_The_Experts_Of_Keyword3", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Borda_Rank_The_Experts_Of_Keyword3", config, topologyBuilder.createTopology());
            Utils.sleep(20 * 1000);
            localCluster.killTopology("Borda_Rank_The_Experts_Of_Keyword3");
            localCluster.shutdown();
        }
    }
}
