package com.spark.examples.worstMovies;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class WorstMovies {

    public static void main(String[] args) {

        /**
         * a simple spark program fo find our worst 100 movies in MovieLens data set
         * this program will be using local resources (both computing and storage) and utilizing Spark RDD only
         */

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf sc = new SparkConf().setAppName("WorstMovies").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> rawData = jsc.textFile("src/main/resources/input/u.data");

        //to calculate average rating of each movie, we need to map movie ID to rating and count of total ratings
        //here we need PairRDD and Tuple
        //Output will be (movieID, (rating, 1.0)) for each movie record (for each line)
        JavaPairRDD<String, Rating> moviesAndRatings = rawData.mapToPair(WorstMovies::splitInfoAndMap);

        //Now, it is time to aggregate ratings by each movie -which is our key
        //Here we can use reduceByKey
        //Output will be (movieID, (sum(rating), sum(1.0))) for each movie - 1 movie 1 line
        JavaPairRDD<String, Rating> moviesAndRatingsAggByMovieId = moviesAndRatings.
                reduceByKey(WorstMovies::sumOfRatingsAndNumberOfRatings);


        /** Exercise! Filter out the movies those have less than 10 ratings */
        JavaPairRDD<String, Rating> filteredMoviesAndRatingsAggByMovieId = moviesAndRatingsAggByMovieId.
                filter(movie -> movie._2.getRatingCount() > 10);
        /** Exercise! Filter out the movies those have less than 10 ratings */

        //We are ready to calculate average rating of each movie
        //It is quite handy to use mapValues function
        //It will map each key in reduced RDD to value we'll be calculating
        //Output will be (movieID, averageRating)
        JavaPairRDD<String, Double> moviesAndAvgRatings = filteredMoviesAndRatingsAggByMovieId.mapValues(movieRating ->
                movieRating.getRating()/movieRating.getRatingCount());

        //Finally sort movies by rating. In our tuple, rating is value and movie id is key
        //In java, sort by value is not supported yet while it is available for python and scala
        //Thats why we have to switch key,value and then do the sorting
        //And finally replace the KV again
        JavaPairRDD<Double, String> worstMoviesKVReplaced = moviesAndAvgRatings.mapToPair(movie ->
                new Tuple2<>(movie._2, movie._1)).sortByKey(true);
        JavaPairRDD<String, Double> worstMoviesSorted = worstMoviesKVReplaced.mapToPair(worstMovie ->
                new Tuple2<>(worstMovie._2, worstMovie._1));

        //Print the result
        List<Tuple2<String, Double>> worstMoviesList = worstMoviesSorted.take(100);
        worstMoviesList.forEach(System.out::println);
    }

    /**
     * Get each line from raw data and convert it to {@link Tuple2}
     * @param line - 1st index is movie ID and 2nd index is Ration
     * @return - MovieID and {@link Rating} as {@link Tuple2}
     */
    private static Tuple2<String, Rating> splitInfoAndMap(String line) {
        String[] splittedMovieInfo = line.split(" ");

        return new Tuple2<>(splittedMovieInfo[1],
                new Rating(Double.parseDouble(splittedMovieInfo[2]), 1.0));
    }

    /**
     * Get 2 movies and calculate the sum({@link Rating#getRating()}) and the sum({@link Rating#getRatingCount()})
     * @param movie1
     * @param movie2
     * @return
     */
    private static Rating sumOfRatingsAndNumberOfRatings(Rating movie1, Rating movie2) {
        return new Rating(movie1.getRating() + movie2.getRating(),
                movie1.getRatingCount() + movie2.getRatingCount());
    }
}
