package Storm.Bolts.Clustering.FuzzyCMeans.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.appengine.repackaged.com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christina on 7/8/2016.
 */
public class EmitFeaturesIntoClosestFuzzyCluster {

    public static double calculateDistanceForFuzzyClustering(double[]vector1,double[]vector2,double[]vector3,double fuzzy){
        double sum=0;

        for(int i=0;i<vector2.length;i++){
            sum+=(vector1[i]-vector2[i])*(vector1[i]-vector2[i])*Math.pow(vector3[i],fuzzy);
        }
        return sum;
    }

    public static double[][]convertSerializedFuzzyCentroidsToFuzzyCentroidsArray(String[]serializedFuzzyCentroids){
        double[][]fuzzyCentroids=new double[3][12];
        for(int i=0;i<serializedFuzzyCentroids.length;i++){
            String serializedFuzzyCentroid=serializedFuzzyCentroids[i];
            String[]splittedSerializedFuzzyCentroid=serializedFuzzyCentroid.split(",");
            List<Double>fuzzyCentroidAsList=new ArrayList<Double>();
            for(int j=0;j<splittedSerializedFuzzyCentroid.length;j++){
                fuzzyCentroidAsList.add(Double.valueOf(splittedSerializedFuzzyCentroid[j]));
            }
            double[]fuzzyCentroid= Doubles.toArray(fuzzyCentroidAsList);
            fuzzyCentroids[i]=fuzzyCentroid;
        }
        return fuzzyCentroids;
    }


    public static void emitFeaturesIntoClosestFuzzyCluster(OutputCollector outputCollector,String[]serializedFuzzyCentroids,String username,List<Double> featuresAsList,double[]featuresAsVector,double[][]memberships,double fuzzy){
        double minValue=Double.MAX_VALUE;
        double diff;
        int index=0;

        double[]deserVector;
        double[]returnVector;
        double[]result;

        double[]fuzzyCentroid=new double[featuresAsVector.length];
        double[]membership=new double[featuresAsVector.length];

        double[][]fuzzyCentroids=convertSerializedFuzzyCentroidsToFuzzyCentroidsArray(serializedFuzzyCentroids);
        for(int i=0;i<fuzzyCentroids.length;i++) {
            fuzzyCentroid = fuzzyCentroids[i];
            membership=memberships[i];
            diff=calculateDistanceForFuzzyClustering(fuzzyCentroid,featuresAsVector,membership,fuzzy);
            if(diff<minValue){
                minValue=diff;
                index=i;
            }
        }
        outputCollector.emit(new Values(index,username,featuresAsList,featuresAsVector,memberships));


    }
}
