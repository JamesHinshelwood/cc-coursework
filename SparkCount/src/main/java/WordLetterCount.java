import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WordLetterCount
{
    private static void createTables(String jdbcUrl) {
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl);

            Statement statement = conn.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS words_spark, letters_spark");

            statement.executeUpdate("CREATE TABLE words_spark (rank INT NOT NULL PRIMARY KEY, word VARCHAR(80), category ENUM('rare', 'popular', 'common'), frequency INT)");

            statement.executeUpdate("CREATE TABLE letters_spark (rank INT NOT NULL PRIMARY KEY, letter VARCHAR(80), category ENUM('rare', 'popular', 'common'), frequency INT)");
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private static void insertValues(String jdbcUrl, String tableName, List<Tuple4<Long, String, String, Integer>> entries) {
        try {
            Connection conn = DriverManager.getConnection(jdbcUrl);

            PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + " (rank, word, category, frequency) VALUES (?, ?, ?, ?)");

            for(Tuple4<Long, String, String, Integer> entry : entries) {
                ps.setLong(1, entry._1());
                ps.setString(2, entry._2());
                ps.setString(3, entry._3());
                ps.setInt(4, entry._4());
                ps.addBatch();
            }

            ps.executeBatch();
        }
        catch(Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://group7.chazymc9bosl.eu-west-2.rds.amazonaws.com:3306/group7_CloudComputingCoursework?user=master&password=password";
        createTables(jdbcUrl);

        SparkSession spark = SparkSession.builder().appName("WordLetterCount").master("local").getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", "AKIAJ3KLSA4N57YMOPZQ");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", "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");

        JavaRDD<String> lines = spark.read().textFile("s3a://cam-cloud-computing-data-source/data-200MB.txt").javaRDD();
        JavaRDD<String> words = lines.flatMap(line -> Arrays.stream(line.split("[ ,.;:?!\"()\\[\\]{}\\-_]")).filter(s -> (s.trim().length() > 0)).iterator());
        List<Tuple3<Long, String, Integer>> counts = words
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .zipWithIndex()
                .map(t -> new Tuple3<>(t._2, t._1._2, t._1._1))
                .collect();

        int popularThreshold = (int)Math.ceil(counts.size() * 0.05);
        int commonLThreshold = (int)Math.ceil(counts.size() * 0.475);
        int commonUThreshold = (int)Math.ceil(counts.size() * 0.525);
        int rareThreshold = (int)Math.ceil(counts.size() * 0.95);

        List<Tuple4<Long, String, String, Integer>> entries =
            Stream.concat(Stream.concat(counts.subList(0, popularThreshold).stream()
                .map(t -> new Tuple4<>(t._1(), t._2(), "popular", t._3())),
            counts.subList(commonLThreshold, commonUThreshold).stream()
                .map(t -> new Tuple4<>(t._1(), t._2(), "common", t._3()))),
            counts.subList(rareThreshold, counts.size()).stream()
                .map(t -> new Tuple4<>(t._1(), t._2(), "rare", t._3()))).collect(Collectors.toList());

        insertValues(jdbcUrl, "words_spark", entries);

        spark.stop();
    }
}
