# spark-bucketing
Sample code to test bucketing performance on Spark.

Bucketing can help improve performance by preshuffling the data in write stage.
If you have multiple jobs which read/shuffle the data post the write stage, then there may be a performance benefit
