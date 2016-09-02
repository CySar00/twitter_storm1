package Storm.Bolts.Clustering.GaussianMixtureModel.Functions;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import com.google.appengine.repackaged.com.google.common.primitives.Doubles;
import com.google.appengine.repackaged.com.google.common.primitives.Floats;
import com.sun.tools.javac.util.List;
import com.sun.tools.javap.LocalVariableTableWriter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christina on 8/8/2016.
 */
public class GaussianMixtureModel {

    public static void calculateTheGaussianMixtureModelOfAHashmap(OutputCollector outputCollector,int clsuterIndex,Map<String,double[]> map){
        float minValue=Float.MAX_VALUE;
        float logLikelihood1;float logLikelihood2;
        int index=0;

        int noOfKeys=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheKeySizeOfAHashmap(map);
        int noOfValues=CalcluateKeySizeAndValuesSizeOfHashmap.calculateTheValuesSizeOfAHashmap(map);

        Map<String,float[]>gaussianDistribution=new HashMap<String, float[]>();
        Map<String,float[]>gaussianProbabilities=new HashMap<String, float[]>();
        float[] Nk=new float[noOfValues];
        float[]muk=new float[noOfValues];
        float[]sigmak=new float[noOfValues];
        float[]pk=new float[noOfValues];

        float[]mu=CalculateMeansAndVariance.calculateTheMeansOfValuesOfAHashmap(map);
        float[]sigma=CalculateMeansAndVariance.calculateTheCovarianceOfValuesOfAHashmap(map);
        float[]p=CalculateMeansAndVariance.calculateProbabilities(noOfValues);


        gaussianDistribution=Gaussian.calculateTheGaussianDistribtionOfAHashmap(map,mu,sigma);
        logLikelihood2=minValue;
        do{
            logLikelihood1=logLikelihood2;
            gaussianProbabilities=Gaussian.calculateTheGaussianProbabilityOfAHashmap(gaussianDistribution,p);

            Nk=Gaussian.calculateTheNk(gaussianProbabilities);
            muk=Gaussian.calculateTheMuk(map,gaussianProbabilities,Nk);
            sigmak=Gaussian.calculateTheSigmaK(map,gaussianProbabilities,muk,Nk);
            pk=Gaussian.calculateTheGaussianPosteriorProbability(Nk);

            mu=muk;
            sigma=sigmak;
            p=pk;

            Map<String,float[]>gaussianDistributionk=Gaussian.calculateTheGaussianDistribtionOfAHashmap(map,mu,sigma);
            logLikelihood2=Gaussian.calculateTheTotalLogLikelihoodOfAHashmap(Gaussian.calculateTheLogLikelihoodUsingTheGaussianDistribution(gaussianDistribution,p));

        }while ((Math.abs(logLikelihood1-logLikelihood2)/Math.abs(logLikelihood1)>0.1));
        Map<String, java.util.List<Float>>gaussianProbabilities1=new HashMap<String,java.util.List<Float>>();
        for(String key:gaussianProbabilities.keySet()){
            float[]valuesOfKey=gaussianProbabilities.get(key);
            gaussianProbabilities1.put(key,Floats.asList(valuesOfKey));

        }

        outputCollector.emit(new Values(clsuterIndex,gaussianProbabilities1));

    }
}
