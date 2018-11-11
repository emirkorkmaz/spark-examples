package com.spark.examples.worstMovies;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.spark.sql.functions.*;

public class WorstMoviesDatasetCassandra {

    /**
     * a simple spark program fo find our worst 100 movies in MovieLens data set
     * this program will be using local resources for reading input and utilizing local Spark for doing aggregations
     * <p>
     * Here the key difference is utilizing Cassandra for final
     */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        //for submitting to Spark on Hadoop
        //setMaster("yarn")
        SparkConf sc = new SparkConf().setAppName("Worst Movies to Cassandra").setMaster("local[*]");
        sc.set("spark.cassandra.connection.host", "95.216.195.156");
        sc.set("spark.cassandra.auth.username", "cassandra");
        sc.set("spark.cassandra.auth.password", "cassandra");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SparkSession ss = SparkSession.builder().master("local[*]").appName("Worst Movies to Cassandra").getOrCreate();

        //or read from HDFS hdfs:///user/cloudera/ml100k/u.data
        JavaRDD<String> raw = jsc.textFile("src/main/resources/input/u.data");
        JavaRDD<Row> movieRatings = raw.map(WorstMoviesDatasetCassandra::convertMovieLineToRow);

        StructType movieRatingsST = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("userid", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("movieid", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("rating", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                DataTypes.createStructField("id", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> movieRatingsDS = ss.createDataFrame(movieRatings, movieRatingsST);
        Dataset<Row> movieRatingsDSExt = movieRatingsDS.
                withColumn("id", col("id").cast("integer")).
                withColumn("movieid", col("movieid").cast("integer")).
                withColumn("rating", col("rating").cast("double")).
                withColumn("timestamp", col("timestamp").cast("timestamp")).
                withColumn("userid", col("userid").cast("integer"));

        Map<String, String> movieRatingsCsOpt = new HashMap<>();
        movieRatingsCsOpt.put("table", "movie_ratings");
        movieRatingsCsOpt.put("keyspace", "movielens");

        movieRatingsDSExt.write().
                format("org.apache.spark.sql.cassandra").
                mode("append").
                options(movieRatingsCsOpt).
                save();

        RelationalGroupedDataset movieRatingsByMovieID = movieRatingsDSExt.groupBy(col("movieid"));
        Dataset<Row> ratingCountByMovieID = movieRatingsByMovieID.count();
        Dataset<Row> avgRatingByMovie = movieRatingsByMovieID.agg(avg(col("rating")).as("avg_rating"));

        Dataset<Row> worstMoviesFiltered = avgRatingByMovie.join(ratingCountByMovieID, "movieid").
                filter(col("count").$greater(10)).select(col("movieid"), col("avg_rating")).
                orderBy(col("avg_rating").asc());

        Map<String, String> worstMoviesCsOpt = new HashMap<>();
        worstMoviesCsOpt.put("table", "worst_movies");
        worstMoviesCsOpt.put("keyspace", "movielens");

        worstMoviesFiltered.write().
                format("org.apache.spark.sql.cassandra").
                mode("append").
                options(worstMoviesCsOpt).save();

        //read back just for fun
        Dataset<Row> worstMoviesReadBack = ss.read().
                format("org.apache.spark.sql.cassandra").
                options(worstMoviesCsOpt).
                load();
        worstMoviesReadBack.registerTempTable("worst_movies_read");
        Dataset<Row> best10OfWorstMovies = ss.sql("SELECT * from worst_movies_read " +
                "order by avg_rating desc " +
                "limit 10");
        best10OfWorstMovies.show();
    }

    private static Row convertMovieLineToRow(String line) {
        //shitty code but works :))
        line += " ";
        line += new Random().nextInt(50000) + 1;
        return RowFactory.create(line.split(" "));
    }
}
