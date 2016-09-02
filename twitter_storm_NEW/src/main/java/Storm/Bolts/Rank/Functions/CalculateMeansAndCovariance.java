package Storm.Bolts.Rank.Functions;

import java.util.Map;

/**
 * Created by christina on 29/8/2016.
 */
public class CalculateMeansAndCovariance {

    public static int calculateNoOfKeysOfAHashmap(Map<String,double[]>map){
        int count=0;
        for(String key:map.keySet()){
            count+=1;
        }
        return count;
    }

    public static int calculateNoOfVectorElementsOfAHashmap(Map<String,double[]>map){
        int sum=0;
        for(Map.Entry<String,double[]>mapEntry:map.entrySet()){
            sum+=mapEntry.getValue().length;
        }
        return (int)sum/calculateNoOfKeysOfAHashmap(map);
    }

    public static double calculateMeansOfAVector(double[] vector){
        double sum=0;
        for(int i=0;i<vector.length;i++){
            sum+=vector[i];
        }
        return sum/vector.length;
    }

    public static double calculateVarianceOfAVector(double[]vector){
        double sum=0;
        for(int i=0;i<vector.length;i++){
            sum+=(vector[i]-calculateMeansOfAVector(vector))*(vector[i]-calculateMeansOfAVector(vector));
        }
        return sum/(vector.length-1);
    }

    public static double calculateStandardDeviationOfAVector(double[]vector){
        double sum=0;
        for(int i=0;i<vector.length;i++){
            sum+=(vector[i]-calculateMeansOfAVector(vector))*(vector[i]-calculateMeansOfAVector(vector));
        }
        return Math.sqrt(sum/(vector.length-1));
    }

    public static double[] calculateTheMeansOfAHashmap(Map<String,double[]> map){
        int noOfKeys=calculateNoOfKeysOfAHashmap(map);
        int noOfElements=calculateNoOfVectorElementsOfAHashmap(map);

        double[]means=new double[noOfElements];
        double[]vector=new double[noOfKeys];

        for(int i=0;i<noOfElements;i++){
            int count=0;
            for(String key:map.keySet()){
                vector[count]=map.get(key)[i];
                count+=1;
            }
            means[i]=calculateMeansOfAVector(vector);
        }
        return means;
    }

    public static double[] calculateTheCovarianceOfAHashmap(Map<String,double[]>map){
        int noOfKeys=calculateNoOfKeysOfAHashmap(map);
        int noOfElements=calculateNoOfVectorElementsOfAHashmap(map);

        double[]covariance=new double[noOfElements];
        double[]vector=new double[noOfKeys];

        for(int i=0;i<noOfElements;i++){
            int count=0;
            for(String key:map.keySet()){
                vector[count]=map.get(key)[i];
                count+=1;
            }
            covariance[i]=calculateVarianceOfAVector(vector);
        }
        return covariance;
    }
}
