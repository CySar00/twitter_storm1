package Databases.NoSQL.CassandraDB.PreprocessingAuthorsAndTweetData;

import com.netflix.astyanax.query.ColumnQuery;
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
import me.prettyprint.hector.api.query.QueryResult;

import javax.swing.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 1/8/2016.
 */
public class CassandraSchemaForAuthorsAndTwittterInformation {
    private static StringSerializer stringSerializer=StringSerializer.get();

    public static String [] readAuthorsAndTwitterInformationFromCassandraDB(){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster=HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("twitterKeyspace");
        if(cluster.describeKeyspace("twitterKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("twitterKeyspace","authors_and_their_twitter_data",ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("twitterKeyspace",ThriftKsDef.DEF_STRATEGY_CLASS,1,Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel>consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("author_and_twitter_data",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        Keyspace keyspace=HFactory.createKeyspace("twitterKeyspace",cluster,configurableConsistencyLevel);
        String[] serializedTwitterInfo=new String[1];
        String []twitterInfo={"authors_and_twitter_data-0"};
        try {
            me.prettyprint.hector.api.query.ColumnQuery<String,String,String>columnQuery=HFactory.createStringColumnQuery(keyspace);
            for(int i=0;i<twitterInfo.length;i++){
                columnQuery.setColumnFamily("authors_and_twitter_data").setKey("authors_and_twitter_data").setKey(twitterInfo[i]);
                QueryResult<HColumn<String,String>>result=columnQuery.execute();

                if(result==null){
                    return null;
                }
                HColumn<String,String>column=result.get();

                if(column==null){
                    return null;
                }
                serializedTwitterInfo[i]=column.getValue();


            }
        }catch (HectorException e){
            e.printStackTrace();
        }
        return serializedTwitterInfo;
    }

    public static void writeAuthorsAndTwitterInformationToCassandraDB(int index,String string){
        CassandraHostConfigurator cassandraHostConfigurator=new CassandraHostConfigurator("localhost:9160");
        Cluster cluster= HFactory.getOrCreateCluster("Test Cluster","localhost:9160");

        ConfigurableConsistencyLevel configurableConsistencyLevel=new ConfigurableConsistencyLevel();
        Map<String,HConsistencyLevel> consistencyLevelMap=new HashMap<String, HConsistencyLevel>();
        consistencyLevelMap.put("authors_and_twitter_data",HConsistencyLevel.ONE);
        configurableConsistencyLevel.setReadCfConsistencyLevels(consistencyLevelMap);
        configurableConsistencyLevel.setWriteCfConsistencyLevels(consistencyLevelMap);

        KeyspaceDefinition keyspaceDefinition=cluster.describeKeyspace("twitterKeyspace");
        if(cluster.describeKeyspace("twitterKeyspace")==null){
            ColumnFamilyDefinition columnFamilyDefinition=HFactory.createColumnFamilyDefinition("twitterKeyspace","authors_and_their_twitter_data", ComparatorType.BYTESTYPE);
            KeyspaceDefinition keyspaceDefinition1=HFactory.createKeyspaceDefinition("twitterKeyspace", ThriftKsDef.DEF_STRATEGY_CLASS,1, Arrays.asList(columnFamilyDefinition));
            cluster.addKeyspace(keyspaceDefinition1,true);
        }
        Keyspace keyspace=HFactory.createKeyspace("twitterKeyspace",cluster,configurableConsistencyLevel);
        Mutator<String>mutator=HFactory.createMutator(keyspace,StringSerializer.get());
        try {
            mutator.insert("authors_and_their_twitter_data","authors_and_twitter_data",HFactory.createStringColumn("authors_and_their_twitter_data-"+index,string));
        }catch (HectorException e){
            e.printStackTrace();
        }
    }

}
