package com.spark.examples.worstMovies.worstMoviesSPOY;

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

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class WorstMoviesDatasetSPOY {

    /**
     * a simple spark program fo find our worst 100 movies in MovieLens data set
     * this program will utilize Yarn, HDFS and Spark RDD for reading files and Dataset API for doing aggregations
     */

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sc = new SparkConf().setAppName("WorstMoviesDatasetSPOY_SC");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        SparkSession ss = SparkSession.builder().appName("WorstMoviesDatasetSPOY_SS").getOrCreate();

        JavaRDD<String> rawData = jsc.textFile("hdfs:///user/cloudera/ml100k/u.data");
        JavaRDD<Row> data = rawData.map(WorstMoviesDatasetSPOY::splitInfo);

        StructType moviesRatingStructType = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("userid", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("itemid", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("rating", DataTypes.StringType, false, Metadata.empty()),
                DataTypes.createStructField("timestamp", DataTypes.StringType, true, Metadata.empty())}
        );

        Dataset<Row> dataDS = ss.createDataFrame(data, moviesRatingStructType);
        Dataset<Row> fieldsOfDataCasted = dataDS.withColumn("rating", col("rating").cast("double"));

        RelationalGroupedDataset dataGroupedByMovie = fieldsOfDataCasted.groupBy(col("itemid"));
        Dataset<Row> worstHundredMovies = dataGroupedByMovie.agg(avg(col("rating")).as("avg_rating")).
                orderBy(col("avg_rating").asc()).limit(100);

        worstHundredMovies.write().csv("hdfs:///user/cloudera/ml100k/worstmovies_by_dataset.dat");

        ss.close();
    }

    private static Row splitInfo(String line) {
        return RowFactory.create(line.split(" "));
    }
}