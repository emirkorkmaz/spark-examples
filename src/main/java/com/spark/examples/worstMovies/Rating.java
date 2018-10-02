package com.spark.examples.worstMovies;

import java.io.Serializable;

public class Rating implements Serializable {

    private double rating;
    private double ratingCount;

    public Rating(double rating, double ratingCount) {
        this.rating = rating;
        this.ratingCount = ratingCount;
    }

    public double getRating() {
        return rating;
    }

    public double getRatingCount() {
        return ratingCount;
    }
}
