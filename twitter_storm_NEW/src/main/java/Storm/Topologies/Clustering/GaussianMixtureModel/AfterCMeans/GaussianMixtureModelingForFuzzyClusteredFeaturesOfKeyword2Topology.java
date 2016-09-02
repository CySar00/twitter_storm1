package Storm.Topologies.Clustering.GaussianMixtureModel.AfterCMeans;

import Storm.Bolts.Clustering.GaussianMixtureModel.Functions.GaussianMixtureModelBolt1;
import Storm.Bolts.Clustering.GaussianMixtureModel.GaussianMixtureModelBolt;
import Storm.Bolts.FileWriterBolt;
import Storm.Spouts.Clustering.GaussianMixtureModelingSpout;
import Storm.Spouts.Clustering.ProcessClusteredFeaturesIntoHashmpSpout;
import Storm.Spouts.Clustering.ProcessFuzzyClusteredFeaturesIntoHashmapSpout;
import Storm.Spouts.ReadFileWithOneLineSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 31/8/2016.
 */
public class GaussianMixtureModelingForFuzzyClusteredFeaturesOfKeyword2Topology {

    public static final String PATH_TO_FEATURES_OF_KEYWORD2_CLUSTER_0_AND_FUZZY_CLUSTER_0="/home/christina/twitter_storm_NEW/K_Clustered_Features/After_Fuzzy_Means/For_Fuzzy_Cluster_Index_Equals_0/features_of_keyword2_that_belong_to_cluster_0_and_fuzzy_cluster_0.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD2_CLUSTER_1_AND_FUZZY_CLUSTER_0="/home/christina/twitter_storm_NEW/K_Clustered_Features/After_Fuzzy_Means/For_Fuzzy_Cluster_Index_Equals_0/features_of_keyword2_that_belong_to_cluster_1_and_fuzzy_cluster_0.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD2_AND_FUZZY_CLUSTER_1="/home/christina/twitter_storm_NEW/Fuzzy_Clustered_Features/features_of_keyword2_that_belong_to_fuzzy_cluster_with_index_1.txt";
    public static final String PATH_TO_FEATURES_OF_KEYWORD2_AND_FUZZY_CLUSTER_2="/home/christina/twitter_storm_NEW/Fuzzy_Clustered_Features/features_of_keyword2_that_belong_to_fuzzy_cluster_with_index_2.txt";

    public static final String PATH_TO_GAUSSIAN_PROBABILITIES_OF_KEYWORD_TEXT_FILE="/home/christina/twitter_storm_NEW/Gaussian_Probabilities/gaussian_probabilities_of_fuzzy_clustered_features_of_keyword2.txt";

    public static void main(String[] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();
        topologyBuilder.setSpout("READ_FUZZY_CLUSTERED_FEATURES_OF_KEYWORD",new ProcessFuzzyClusteredFeaturesIntoHashmapSpout(PATH_TO_FEATURES_OF_KEYWORD2_CLUSTER_0_AND_FUZZY_CLUSTER_0,PATH_TO_FEATURES_OF_KEYWORD2_CLUSTER_1_AND_FUZZY_CLUSTER_0,PATH_TO_FEATURES_OF_KEYWORD2_AND_FUZZY_CLUSTER_1,PATH_TO_FEATURES_OF_KEYWORD2_AND_FUZZY_CLUSTER_2));
        topologyBuilder.setBolt("GAUSSIAN_MIXTURE_MODELING_FUZZY_CLUSTERED_FEATURES",new GaussianMixtureModelBolt1()).shuffleGrouping("READ_FUZZY_CLUSTERED_FEATURES_OF_KEYWORD");
        topologyBuilder.setBolt("WRITE_GAUSSIAN_PROBABILITIES_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_GAUSSIAN_PROBABILITIES_OF_KEYWORD_TEXT_FILE)).shuffleGrouping("GAUSSIAN_MIXTURE_MODELING_FUZZY_CLUSTERED_FEATURES");

        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Gaussian_Mixture_Modeling_Fuzzy_Clustered_Features_Of_Keyword2", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Gaussian_Mixture_Modeling_Fuzzy_Clustered_Features_Of_Keyword2", config, topologyBuilder.createTopology());
            Utils.sleep(300*1000);
            localCluster.killTopology("Gaussian_Mixture_Modeling_Fuzzy_Clustered_Features_Of_Keyword2");
            localCluster.shutdown();
        }



    }
}
