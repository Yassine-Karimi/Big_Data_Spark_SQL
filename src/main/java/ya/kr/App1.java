package ya.kr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.DatagramSocket;

public class App1 {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder()
                .appName("Exo1")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = ss.read().format("csv").option("header", true).load("src/main/resources/incidents.csv");
        df.createOrReplaceTempView("incidents");

        System.out.println("----------------------Afficher le nombre d’incidents par service-----------------------");
        Dataset<Row> dfSQL1= ss.sql("select service,count(service) from incidents group by service");
        // ou bien
        Dataset<Row> incidentsByService = df.withColumn("service", df.col("service"));
        Dataset<Row> incidentsCountByService = incidentsByService.groupBy("service").count();
        incidentsCountByService.show();
        //dfSQL1.show();


        System.out.println("----------------------Afficher les deux années où il a y avait plus d’incidents.-----------------------");
        Dataset<Row> incidentsByYear = df.withColumn("year", df.col("date").substr(0, 4));
        Dataset<Row> incidentsCountByYear = incidentsByYear.groupBy("year").count();
        incidentsCountByYear = incidentsCountByYear.sort(incidentsCountByYear.col("count").desc());
        incidentsCountByYear.show(2);

    }
}