package ya.kr;

import org.apache.spark.sql.SparkSession;

public class App2 {

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Exo1")
                .master("local[*]")
                .getOrCreate();


    }
}
