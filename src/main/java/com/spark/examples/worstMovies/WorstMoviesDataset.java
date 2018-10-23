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

import static org.apache.spark.sql.functions.*;

public class WorstMoviesDataset {

    /**
     * a simple spark program fo find our worst 100 movies in MovieLens data set
     * this program will be using local resources (both computing and storage) and utilizing Spark RDD
     * for reading files and Dataset API for doing aggregations
     */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sc = new SparkConf().setAppName("WorstMoviesDataset").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        SparkSession ss = SparkSession.builder().master("local[*]").appName("WorstMoviesDataset").getOrCreate();

        JavaRDD<String> rawData = jsc.textFile("src/main/resources/input/u.data");
        JavaRDD<Row> data = rawData.map(WorstMoviesDataset::splitInfo);

        StructType moviesRatingStructType = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("userid", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("movieid", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("rating", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("timestamp", DataTypes.StringType, true, Metadata.empty())}
        );

        Dataset<Row> dataDS = ss.createDataFrame(data, moviesRatingStructType);
        Dataset<Row> fieldsOfDataCasted = dataDS.withColumn("rating", col("rating").cast("double"));

        RelationalGroupedDataset dataGroupedByMovie = fieldsOfDataCasted.groupBy(col("movieid"));
        Dataset<Row> worstHundredMovies = dataGroupedByMovie.
                agg(avg(col("rating")).as("avg_rating"));

        /** Exercise! Filter out the movies those have less than 10 ratings */
        Dataset<Row> movieCount = dataGroupedByMovie.count();
        Dataset<Row> filteredWorstMovies = worstHundredMovies.join(movieCount,"movieid").
                filter(col("count").$greater(10)).select(col("movieid"), col("avg_rating")).
                orderBy(col("avg_rating").asc()).limit(100);
        /** Exercise! Filter out the movies those have less than 10 ratings */

        filteredWorstMovies.show();
        //worstHundredMovies.write().csv("src/main/resources/output/worst100moviesByDS.text");

        ss.close();
    }

    private static Row splitInfo(String line) {
        return RowFactory.create(line.split(" "));
    }
}