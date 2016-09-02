package Storm.Topologies.RankResults.CheckingGaussianProbabilities;

import Storm.Bolts.FileWriterBolt;
import Storm.Bolts.Rank.CheckGaussianProbabilitiesBolt;
import Storm.Spouts.Rank.ProcessMapsWithGaussianProbabilitiesSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 11/8/2016.
 */
public class CheckingGaussianProbabilitiesOfKeyword1Topology {
    public static final String PATH_TO_GAUSSIAN_PROBABILITIES_OF_CLUSTERED_FEATURES="/home/christina/twitter_storm_NEW/Gaussian_Probabilities/gaussian_probabilitities_of_clustered_features_of_keyword1.txt";

    public static final String PATH_TO_CHECKED_GAUSSIAN_PROBABILITIES_OF_CLUSTER_0="/home/christina/twitter_storm_NEW/Checked_Gaussian_Probabilities/gaussian_probabilities_of_features_of_cluster_0_and_keyword1.txt";
    public static final String PATH_TO_CHECKED_GAUSSIAN_PROBABILITIES_OF_CLUSTER_1="/home/christina/twitter_storm_NEW/Checked_Gaussian_Probabilities/gaussian_probabilities_of_features_of_cluster_1_and_keyword1.txt";


    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("PROCESS_GAUSSIAN_PROBABILITIES_FROM_TEXT_FILE",new ProcessMapsWithGaussianProbabilitiesSpout(PATH_TO_GAUSSIAN_PROBABILITIES_OF_CLUSTERED_FEATURES));

        topologyBuilder.setBolt("CHECK_GAUSSIAN_PROBABILITIES_OF_CLUSTER_WITH_INDEX_0",new CheckGaussianProbabilitiesBolt(0)).shuffleGrouping("PROCESS_GAUSSIAN_PROBABILITIES_FROM_TEXT_FILE");
        topologyBuilder.setBolt("WRITE_CHECKED_GAUSSIAN_PROBABILITIES_OF_CLUSTER_WITH_INDEX_0_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_CHECKED_GAUSSIAN_PROBABILITIES_OF_CLUSTER_0)).shuffleGrouping("CHECK_GAUSSIAN_PROBABILITIES_OF_CLUSTER_WITH_INDEX_0");

        topologyBuilder.setBolt("CHECK_GAUSSIAN_PROBABILITIES_OF_CLUSTER_WITH_INDEX_1",new CheckGaussianProbabilitiesBolt(1)).shuffleGrouping("PROCESS_GAUSSIAN_PROBABILITIES_FROM_TEXT_FILE");
        topologyBuilder.setBolt("WRITE_CHECKED_GAUSSIAN_PROBABILITIES_OF_CLUSTER_WITH_INDEX_1_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_CHECKED_GAUSSIAN_PROBABILITIES_OF_CLUSTER_1)).shuffleGrouping("CHECK_GAUSSIAN_PROBABILITIES_OF_CLUSTER_WITH_INDEX_1");




        Config config = new Config();
        if (args != null && args.length > 0) {
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Checking_Gaussian_Probabilities_Of_Features_Of_Keyword1", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Checking_Gaussian_Probabilities_Of_Features_Of_Keyword1", config, topologyBuilder.createTopology());
            Utils.sleep(2*10*1000);
            localCluster.killTopology("Checking_Gaussian_Probabilities_Of_Features_Of_Keyword1");
            localCluster.shutdown();
        }
    }
}
