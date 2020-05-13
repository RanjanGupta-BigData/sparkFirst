import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
 
public class PrintRDD {
 
    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD")
                                        .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        
        // read text files to RDD
        JavaRDD<String> lines = sc.textFile("C:\\Spark-Learn\\spark-2.4.4-bin-hadoop2.6\\data1\\data.txt");
        
        // collect RDD for printing
        for(String line:lines.collect()){
            System.out.println("* "+line);
        }
        lines.foreach(new VoidFunction<String>(){ 
            public void call(String line) {
                System.out.println("* "+line); 
      }});
    }
}
