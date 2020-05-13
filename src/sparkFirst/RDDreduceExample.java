package sparkFirst;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 
public class RDDreduceExample {
 
    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                                        .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // read text file to RDD
        JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(14,21,88,99,455));
        
        // aggregate numbers using addition operator
        int sum = numbers.reduce((a,b)->a+b); 
        
        System.out.println("Sum of numbers is : "+sum);
    }
 
}