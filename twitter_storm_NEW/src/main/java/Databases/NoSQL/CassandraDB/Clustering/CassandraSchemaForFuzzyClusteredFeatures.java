package Databases.NoSQL.CassandraDB.Clustering;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 31/8/2016.
 */
public class CassandraSchemaForFuzzyClusteredFeatures {
    private static StringSerializer stringSerializer=new StringSerializer();

    public static String[] readClusteredFeaturesHashmapToCassandraDB(){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster= HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("myFuzzyClusteredFeaturesKeyspace2");
        if(keyspaceDefinition==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("myFuzzyClusteredFeaturesKeyspace2","fuzzy_clustered_features", ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("myFuzzyClusteredFeaturesKeyspace2", ThriftKsDef.DEF_STRATEGY_CLASS,1, Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel> consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("fuzzy_clustered_features",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        Keyspace keyspace=HFactory.createKeyspace("myFuzzyClusteredFeaturesKeyspace2",cluster,configurableConsistencyLevel);
        String[]serializedHashmapsOfClusteredFeatures=new String[2];
        String [] clusteredHashmapOfFeatures={"fuzzy_clustered_hashmap_of_features-0","fuzzy_clustered_hashmap_of_features-1","fuzzy_clustered_hashmap_of_features-2","fuzzy_clustered_hashmap_of_features-3"};
        try {
            ColumnQuery<String,String,String> columnQuery=HFactory.createStringColumnQuery(keyspace);
            for(int i=0;i<clusteredHashmapOfFeatures.length;i++){
                columnQuery.setColumnFamily("fuzzy_clustered_features").setKey("fuzzy_clustered_hashmap_of_features").setName(clusteredHashmapOfFeatures[i]);
                QueryResult<HColumn<String,String>> result=columnQuery.execute();
                if(result==null){
                    return  null;
                }
                HColumn<String,String>column=result.get();
                if(column==null){
                    return null;
                }
                serializedHashmapsOfClusteredFeatures[i]=column.getValue();
            }
        }catch (HectorException e){
            e.printStackTrace();
        }

        return serializedHashmapsOfClusteredFeatures;
    }

    public static void writeClusteredFeaturesHashmapToCassandraDB(int index,String serializedClusteredFeatures){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster= HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("fuzzy_clustered_features",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("myFuzzyClusteredFeaturesKeyspace2");
        if(keyspaceDefinition==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("myFuzzyClusteredFeaturesKeyspace2","fuzzy_clustered_features", ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("myFuzzyClusteredFeaturesKeyspace2", ThriftKsDef.DEF_STRATEGY_CLASS,1, Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        Keyspace  keyspace=HFactory.createKeyspace("myFuzzyClusteredFeaturesKeyspace2",cluster,configurableConsistencyLevel);
        Mutator<String> mutator=HFactory.createMutator(keyspace,stringSerializer);
        try {
            mutator.insert("fuzzy_clustered_hashmap_of_features","fuzzy_clustered_features",HFactory.createStringColumn("fuzzy_clustered_hashmap_of_features-"+index,serializedClusteredFeatures));
        }catch (HectorException e){
            e.printStackTrace();
        }

    }
}
