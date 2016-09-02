package Databases.NoSQL.CassandraDB.PreprocessingAuthorsAndTweetData;

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

import javax.ws.rs.HEAD;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 26/7/2016.
 */
public class CassandraSchemaForAuthorsAndFeatures {

    public static String[] readFeaturesFromCassandraDB(){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("featuresKeyspace");
        if(cluster.describeKeyspace("featuresKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("featuresKeyspace","authors_and_features_hashmap", ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("featuresKeyspace", ThriftKsDef.DEF_STRATEGY_CLASS,1, Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel> consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("authors_and_features_hashmap",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        Keyspace keyspace=HFactory.createKeyspace("featuresKeyspace",cluster,configurableConsistencyLevel);
        String [] serializedHashmapOfFeatures=new String[1]; String[] features={"features"};
        try {
            ColumnQuery<String,String,String>columnQuery=HFactory.createStringColumnQuery(keyspace);
            for(int i=0;i<serializedHashmapOfFeatures.length;i++){
                columnQuery.setColumnFamily("authors_and_features_hashmap").setKey("features").setName(features[i]);
                QueryResult<HColumn<String,String>>result=columnQuery.execute();

                if(result==null){
                    return null;
                }
                HColumn<String,String>column=result.get();
                if(column ==null){
                    return  null;
                }
                serializedHashmapOfFeatures[i]=column.getValue();
            }
        }catch (HectorException e){
            e.printStackTrace();
        }

        return serializedHashmapOfFeatures;
    }

    public static void writeFeaturesToCassandraDB(String string){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster= HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("authors_and_features_hashmap",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("featuresKeyspace");
        if(cluster.describeKeyspace("featuresKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("featuresKeyspace","authors_and_features_hashmap",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("featuresKeyspace",ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        Keyspace keyspace=HFactory.createKeyspace("featuresKeyspace",cluster,configurableConsistencyLevel);
        Mutator<String>mutator=HFactory.createMutator(keyspace, StringSerializer.get());
        try {
            mutator.insert("features","authors_and_features_hashmap",HFactory.createStringColumn("features",string));
        }catch (HectorException e){
            e.printStackTrace();
        }


    }


}
