package Storm.Topologies.RankResults.GaussianRank;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.CalculateCDFsOfExpertsFeaturesBolt;
import Storm.Bolts.Rank.GaussRankTheExpertsBolt;
import Storm.Spouts.Rank.ProcessExpertsAndTheirCDFsSpout;
import Storm.Spouts.Rank.ProcessTheExpertsAndTheirFeaturesSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 29/8/2016.
 */
public class RankExpertsOfKeyword1Topology {
    public static final String PATH_TO_EXPERTS_CDFS="/home/christina/twitter_storm_NEW/CDFs_Of_Features/experts_and_cdfs_of_features_of_keyword1.txt";
    public static final String PATH_TO_RANKED_AUTHORS="/home/christina/twitter_storm_NEW/Gauss_Ranked_Experts/ranked_experts_of_keyword1.txt";


    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("PROCESS_EXPERTS_AND_CDFS_OF_FEATURES",new ProcessExpertsAndTheirCDFsSpout(PATH_TO_EXPERTS_CDFS));
        topologyBuilder.setBolt("GAUSS_RANK_THE_EXPERTS",new GaussRankTheExpertsBolt()).shuffleGrouping("PROCESS_EXPERTS_AND_CDFS_OF_FEATURES");
        topologyBuilder.setBolt("WRITE_RANKED_EXPERTS_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_RANKED_AUTHORS)).shuffleGrouping("GAUSS_RANK_THE_EXPERTS");



        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Calculating_The_CDFs_Of_The_Experts_Of_Keyword1", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Calculating_The_CDFs_Of_The_Experts_Of_Keyword1", config, topologyBuilder.createTopology());
            Utils.sleep(20 * 1000);
            localCluster.killTopology("Calculating_The_CDFs_Of_The_Experts_Of_Keyword1");
            localCluster.shutdown();
        }
    }
}
