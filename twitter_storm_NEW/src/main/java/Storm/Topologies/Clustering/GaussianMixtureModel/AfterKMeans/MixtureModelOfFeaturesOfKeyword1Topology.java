package Storm.Topologies.Clustering.GaussianMixtureModel.AfterKMeans;

import Storm.Bolts.Clustering.GaussianMixtureModel.GaussianMixtureModelBolt;
import Storm.Bolts.FileWriterBolt;
import Storm.Spouts.Clustering.GaussianMixtureModelingSpout;
import Storm.Spouts.Clustering.ProcessClusteredFeaturesIntoHashmpSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by christina on 7/8/2016.
 */
public class MixtureModelOfFeaturesOfKeyword1Topology {
    public static final String PATH_TO_CLUSTERED_FEATURES_OF_KEYWORD1_AND_CLUSTER_0="/home/christina/twitter_storm_NEW/K_Clustered_Features/features_of_keyword1_that_belong_to_cluster_with_index_0.txt";
    public static final String PATH_TO_CLUSTERED_FEATURES_OF_KEYWORD1_AND_CLUSTER_1="/home/christina/twitter_storm_NEW/K_Clustered_Features/features_of_keyword1_that_belong_to_cluster_with_index_1.txt";

    public static final String PATH_TO_GAUSSIAN_PROBABILITIES_OF_CLUSTERED_FEATURES="/home/christina/twitter_storm_NEW/Gaussian_Probabilities/gaussian_probabilitities_of_clustered_features_of_keyword1.txt";

    public static void main(String[] args) throws Exception{
        TopologyBuilder topologyBuilder=new TopologyBuilder();

        topologyBuilder.setSpout("PROCESS_CLUSTERED_FEATURES_INTO_HASHMAP",new ProcessClusteredFeaturesIntoHashmpSpout(PATH_TO_CLUSTERED_FEATURES_OF_KEYWORD1_AND_CLUSTER_0,PATH_TO_CLUSTERED_FEATURES_OF_KEYWORD1_AND_CLUSTER_1));
        topologyBuilder.setSpout("EMIT_FEATURES_OF_KEYWORD1_FOR_GAUSSIAN_MIXTURE_MODELING",new GaussianMixtureModelingSpout());
        topologyBuilder.setBolt("GAUSSIAN_MIXTURE_MODELING_FEATURES_OF_KEYWORD1",new GaussianMixtureModelBolt()).shuffleGrouping("EMIT_FEATURES_OF_KEYWORD1_FOR_GAUSSIAN_MIXTURE_MODELING");
        topologyBuilder.setBolt("WRITE_GAUSSIAN_PROBABILITIES_TO_TEXT_FILE",new FileWriterBolt(PATH_TO_GAUSSIAN_PROBABILITIES_OF_CLUSTERED_FEATURES)).shuffleGrouping("GAUSSIAN_MIXTURE_MODELING_FEATURES_OF_KEYWORD1");


        Config config=new Config();
        if(args!=null && args.length>0){
            config.setNumWorkers(5);
            config.setNumAckers(10);
            config.setMaxSpoutPending(100);
            StormSubmitter.submitTopology("Gaussian_Mixture_Modeling_Features_Of_Keyword1", config, topologyBuilder.createTopology());
        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("Gaussian_Mixture_Modeling_Features_Of_Keyword1", config, topologyBuilder.createTopology());
            Utils.sleep(1);
            localCluster.killTopology("Gaussian_Mixture_Modeling_Features_Of_Keyword1");
            localCluster.shutdown();
        }



    }
}
