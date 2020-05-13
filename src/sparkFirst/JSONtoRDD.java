package sparkFirst;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
 
public class JSONtoRDD {
 
    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Read JSON to RDD")
                .master("local[2]")
                .getOrCreate();
 
        // read list to RDD
        String jsonPath = "C:\\Spark-Learn\\spark-2.4.4-bin-hadoop2.6\\data1\\employees.json";
        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();
        
        items.foreach(item -> {
            System.out.println(item); 
        });
    }
}
