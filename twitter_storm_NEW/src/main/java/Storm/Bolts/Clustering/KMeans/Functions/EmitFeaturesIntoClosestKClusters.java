package Storm.Bolts.Clustering.KMeans.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.appengine.repackaged.com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christina on 4/8/2016.
 */
public class EmitFeaturesIntoClosestKClusters {

    public static double calculateEuclideanDistanceBetweenTwoDoubleVectors(double[] vector1,double[] vector2){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=(vector1[i]-vector2[i])*(vector1[i]-vector2[i]);
        }
        return Math.sqrt(sum);
    }

    public static double calculateDistanceBetweenTwoDoubleVectors(double[]vector1,double[]vector2){
        double sum=0;

        for(int i=0;i<vector1.length;i++){
            sum+=Math.abs(vector1[i]-vector2[i]);
        }

        return sum;
    }

   public static void emitFeaturesIntoClusterWithClosestCentroid(OutputCollector outputCollector,String[] serializedCentroids,String author,double[] featuresAsVector,List<Double>featuresAsList){
       Double minValue=Double.MAX_VALUE;
       double diff;
       int index=0;

       double[]deserVector;
       double[]returnVector;
       double[]result;
       double[]centroid;

       for(int i=0;i<serializedCentroids.length;i++) {
           String serializedCentroid = serializedCentroids[i];
           String[] splittedSerializedCentroid=serializedCentroid.split(",");
           List<Double>centroidAsList=new ArrayList<Double>();
           for(int j=0;j<featuresAsVector.length;j++){
               centroidAsList.add(Double.valueOf(splittedSerializedCentroid[j]));
           }
           centroid=Doubles.toArray(centroidAsList);
           diff=calculateEuclideanDistanceBetweenTwoDoubleVectors(centroid,featuresAsVector);

           if(diff<minValue){
               minValue=diff;
               index=i;
           }
       }
       System.out.println(index);
       outputCollector.emit(new Values(index,author,featuresAsVector,featuresAsList));
   }
}
