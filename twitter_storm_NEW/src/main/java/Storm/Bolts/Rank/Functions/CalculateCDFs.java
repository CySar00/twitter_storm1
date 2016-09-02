package Storm.Bolts.Rank.Functions;

import backtype.storm.task.OutputCollector;

import java.util.Map;

/**
 * Created by christina on 12/8/2016.
 */
public class CalculateCDFs {

    public static double erf(double x){
        double t=1.0/(1.0+0.5*Math.abs(x));

        //use horner's method
        double ans=1-t*Math.exp(-x*x-1.26551223+
                t*(1.0002368+ t*(0.37409196+ t*(0.09678418+ t*(-0.18628806 +
                        t*(0.27886806+ t*(-1.1352098))+t*(1.48851587 +t*(-0.82215223+t*(0.17087277))))))));

        if (x>=0){
            return ans;
        }else{
            return -ans;
        }
    }

    public static double erf2(double x){
        double t=1.0/(1.0+0.47047*Math.abs(x));
        double poly=t*(0.3480242+t*(-0.0958798+t*(0.7478556)));

        double ans=1.0-poly*Math.exp(-x*x);

        if(x>=0){
            return ans;
        }else{
            return -ans;
        }
    }

    public static double Phi(double z){
        return 0.5*(1.0+erf(z/(Math.sqrt(2.0))));
    }



    public static void calculateTheCDFsOfTheExperts(OutputCollector outputCollector, Map<String,float[]>map){

    }
}
