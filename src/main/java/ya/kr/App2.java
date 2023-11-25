package ya.kr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.sql.DatabaseMetaData;

public class App2 {

    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder()
                .appName("Exo1")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = ss.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "CONSULTATIONS")
                .option("user", "root")
                .option("password", "")
                .load();
       df.createOrReplaceTempView("consultations");
        Dataset<Row> patientsBySex = ss.sql("SELECT DATE, COUNT(*) AS COUNT FROM consultations GROUP BY DATE");
        patientsBySex.show();

        Dataset<Row> df1 = ss.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "MEDECINS")
                .option("user", "root")
                .option("password", "")
                .load();
        df1.createOrReplaceTempView("medecins");
        Dataset<Row> ConsultationssByDoc = ss.sql("SELECT medecins.nom, medecins.prenom, COUNT(*) AS NumConsultationsPerDoc\n" +
                "FROM consultations\n" +
                "INNER JOIN medecins ON consultations.id_medecin = medecins.id\n" +
                "GROUP BY medecins.nom, medecins.prenom;");
        ConsultationssByDoc.show();


        Dataset<Row> patientsPerDo = ss.sql("SELECT medecins.nom, medecins.prenom, COUNT(DISTINCT consultations.id_patient) AS N_PatientsPerDoc\n" +
                "FROM consultations\n" +
                "INNER JOIN medecins ON consultations.id_medecin = medecins.id\n" +
                "GROUP BY medecins.nom, medecins.prenom;");

        patientsPerDo.show();

    }
}
