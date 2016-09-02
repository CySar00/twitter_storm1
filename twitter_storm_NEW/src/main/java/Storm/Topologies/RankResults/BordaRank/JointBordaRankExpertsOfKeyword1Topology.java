package Storm.Topologies.RankResults.BordaRank;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.BordaRankTheExpertsBolt;
import Storm.Bolts.Rank.ReRankTheExpertsUsingBordaScoresBolt;
import Storm.Spouts.Rank.Borda.JoinBordaRanksFromDifferentFuzzyClustersSpout;
import Storm.Spouts.Rank.Borda.ProcesssTheExpertsAndTheirFeaturesSpout1;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 1/9/2016.
 */
public class JointBordaRankExpertsOfKeyword1Topology {

    public static final String RANKED_EXPERTS_OF_KEYWORD1="/home/christina/twitter_storm_NEW/Borda_Ranked_Experts/Ranked_Experts_Of_Keyword1_And_Fuzzy_Cluster0.txt";
    public static final String RANKED_EXPERTS_OF_KEYWORD2="/home/christina/twitter_storm_NEW/Borda_Ranked_Experts/Ranked_Experts_Of_Keyword1_And_Fuzzy_Cluster1.txt";

    public static final String RANKED_EXPERTS_OF_KEYWORD="/home/christina/twitter_storm_NEW/Borda_Ranked_Experts/Ranked_Experts_Of_Keyword1.txt";

    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("PROCESS_BORDA_SCORES_FROM_TEXT_FILES",new JoinBordaRanksFromDifferentFuzzyClustersSpout(RANKED_EXPERTS_OF_KEYWORD1,RANKED_EXPERTS_OF_KEYWORD2));
        topologyBuilder.setBolt("RE-RANK_BORDA_SCORES",new ReRankTheExpertsUsingBordaScoresBolt()).shuffleGrouping("PROCESS_BORDA_SCORES_FROM_TEXT_FILES");
        topologyBuilder.setBolt("WRITE_RANKED_EXPERTS_TO_TEXT_FILE",new FileWriterBolt(RANKED_EXPERTS_OF_KEYWORD)).shuffleGrouping("RE-RANK_BORDA_SCORES");





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
