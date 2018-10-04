# spark-examples
Various samples with Spark

### Submitting a job to Hadoop Cluster with spark-submit (utilizing spark on yarn)  
* build your application into a .jar file  
* make sure all dependencies are assembled into jar file  
* upload it to Hadoop master node  
* use following command to submit it ``` spark-submit --class com.spark.examples.worstMovies.worstMoviesSPOY.WorstMoviesSPOY --master yarn --deploy-mode cluster /media/shared_from_local/sparkexamples.jar ``` (replace class name and jar with yours)  
* logs can be traced via following command ``` yarn logs -applicationId application_1538675682869_0113 ``` (replace with your application id retrieved from spark-submit output)  
* Job status can be tracked through Hue or Spark UI  
