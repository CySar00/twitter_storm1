package Storm.Bolts.Clustering.FuzzyCMeans.Functions;

import com.google.appengine.repackaged.com.google.common.primitives.Doubles;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christina on 7/8/2016.
 */
public class CalculateMembershipForFuzzyClustering {

    public static double[][] convertSerializedFuzzyCentroidsToFuzzyCentroidsArray(String[]serializedFuzzyCentroids){
        double[][]fuzzyCentroids=new double[3][12];

        for(int i=0;i<serializedFuzzyCentroids.length;i++){
            String serializedFuzzyCentroid=serializedFuzzyCentroids[i];
            String[] splittedSerializedFuzzyCentroid=serializedFuzzyCentroid.split(",");
            double[] fuzzyCentroid=new double[splittedSerializedFuzzyCentroid.length];
            List<Double>fuzzyCentroidAsList=new ArrayList<Double>();

            for(int j=0;j<splittedSerializedFuzzyCentroid.length;j++){
                fuzzyCentroidAsList.add(Double.valueOf(splittedSerializedFuzzyCentroid[j]));
            }
            fuzzyCentroid= Doubles.toArray(fuzzyCentroidAsList);
            fuzzyCentroids[i]=fuzzyCentroid;
        }
        return fuzzyCentroids;
    }


    public static double[][]calculateMembershipForFuzzyClustering(String[]serializedFuzzyCentroids,double[]featuresAsVector,double fuzzy){
        double[][]membership=new double[serializedFuzzyCentroids.length][featuresAsVector.length];
        double[][]fuzzyCentroids=new double[serializedFuzzyCentroids.length][featuresAsVector.length];

        fuzzyCentroids=convertSerializedFuzzyCentroidsToFuzzyCentroidsArray(serializedFuzzyCentroids);
        double powerOfFuzzy=1/(fuzzy-1);

        double totalSum=0;
        for(int i=0;i<fuzzyCentroids.length;i++){
            double[]fuzzyCentroid=fuzzyCentroids[i];
            double sum=0;
            for(int j=0;j<featuresAsVector.length;j++){
                sum+=(fuzzyCentroid[j]-featuresAsVector[j])*(fuzzyCentroid[j]-featuresAsVector[j]);
            }
            totalSum+=Math.sqrt(sum);
        }
        double demOfFuzzy=Math.pow(totalSum,powerOfFuzzy);

        double euclideanDistance;
        for(int i=0;i<fuzzyCentroids.length;i++){
            double[]fuzzyCentroid=fuzzyCentroids[i];
            double sum=0;
            for(int j=0;j<featuresAsVector.length;j++){
                sum+=(fuzzyCentroid[j]-featuresAsVector[j])*(fuzzyCentroid[j]-featuresAsVector[j]);
                euclideanDistance=Math.sqrt(sum);
                membership[i][j]=Math.pow(euclideanDistance,powerOfFuzzy)/demOfFuzzy;
            }
        }

        return membership;
    }
}
