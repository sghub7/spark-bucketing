
mport org.apache.spark.sql.functions._
def cecl_mth = spark.range(1, 10000000).select($"id" as "key1",$"id"*2 as "key2", rand(12) as "value")
def cecl_result = spark.range(1, 5000000).select($"id" as "key1",$"id"*2 as "key2", rand(12) as "value")

// COMMAND ----------

// Unbucketed
cecl_mth.write.format("parquet").saveAsTable("cecl_mth_unbucketed")
cecl_result.write.format("parquet").saveAsTable("cecl_result_unbucketed")

// COMMAND ----------

// DIsable broadcast join just to ensure there is no Broadcast Hash Join
//as We want to simulate for SortMergeJoin here
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

// COMMAND ----------

// Bucketed tables. 16 buckets 
cecl_mth.write.format("parquet").bucketBy(16, "key1").saveAsTable("cecl_mth_bucketed")
cecl_result.write.format("parquet").bucketBy(16, "key1").saveAsTable("cecl_result_bucketed")


// COMMAND ----------

val t1 = spark.table("cecl_mth_unbucketed")
val t2 = spark.table("cecl_result_unbucketed")
val t1_b = spark.table("cecl_mth_bucketed")
val t2_b = spark.table("cecl_result_bucketed")

// COMMAND ----------

// Unbucketed join Plan..
// There are 2 shuffles (exchanges)
t1.join(t2, Seq("key1")).explain()
/**
== Physical Plan ==
*(3) Project [key1#937L, key2#938L, value#939, key2#944L, value#945]
+- *(3) SortMergeJoin [key1#937L], [key1#943L], Inner
   :- Sort [key1#937L ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(key1#937L, 200), [id=#2104]
   :     +- *(1) Project [key1#937L, key2#938L, value#939]
   :        +- *(1) Filter isnotnull(key1#937L)
   :           +- *(1) FileScan parquet default.cecl_mth_unbucketed[key1#937L,key2#938L,value#939] Batched: true, DataFilters: [isnotnull(key1#937L)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/cecl_mth_unbucketed], PartitionFilters: [], PushedFilters: [IsNotNull(key1)], ReadSchema: struct<key1:bigint,key2:bigint,value:double>
   +- Sort [key1#943L ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(key1#943L, 200), [id=#2108]
         +- *(2) Project [key1#943L, key2#944L, value#945]
            +- *(2) Filter isnotnull(key1#943L)
               +- *(2) FileScan parquet default.cecl_result_unbucketed[key1#943L,key2#944L,value#945] Batched: true, DataFilters: [isnotnull(key1#943L)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/cecl_result_unbucketed], PartitionFilters: [], PushedFilters: [IsNotNull(key1)], ReadSchema: struct<key1:bigint,key2:bigint,value:double>
**/

// COMMAND ----------

// Bucketed join. 
// No shuffle (No Exchange step in plan)...
t1_b.join(t2_b, Seq("key1")).explain()

/**
== Physical Plan ==
*(3) Project [key1#949L, key2#950L, value#951, key2#956L, value#957]
+- *(3) SortMergeJoin [key1#949L], [key1#955L], Inner
   :- *(1) Sort [key1#949L ASC NULLS FIRST], false, 0
   :  +- *(1) Project [key1#949L, key2#950L, value#951]
   :     +- *(1) Filter isnotnull(key1#949L)
   :        +- *(1) FileScan parquet default.cecl_mth_bucketed[key1#949L,key2#950L,value#951] Batched: true, DataFilters: [isnotnull(key1#949L)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/cecl_mth_bucketed], PartitionFilters: [], PushedFilters: [IsNotNull(key1)], ReadSchema: struct<key1:bigint,key2:bigint,value:double>, SelectedBucketsCount: 16 out of 16
   +- *(2) Sort [key1#955L ASC NULLS FIRST], false, 0
      +- *(2) Project [key1#955L, key2#956L, value#957]
         +- *(2) Filter isnotnull(key1#955L)
            +- *(2) FileScan parquet default.cecl_result_bucketed[key1#955L,key2#956L,value#957] Batched: true, DataFilters: [isnotnull(key1#955L)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/cecl_result_bucketed], PartitionFilters: [], PushedFilters: [IsNotNull(key1)], ReadSchema: struct<key1:bigint,key2:bigint,value:double>, SelectedBucketsCount: 16 out of 16
**/

// COMMAND ----------

// In the above buckceted join,  There is still a sort step .. We can avoid this by pre sort during save
// Bucketed tables. 16 buckets 
cecl_mth.write.format("parquet").bucketBy(16, "key1").sortBy("key1").saveAsTable("cecl_mth_bucketed_sorted")
cecl_result.write.format("parquet").bucketBy(16, "key1").sortBy("key1").saveAsTable("cecl_result_bucketed_sorted")

// COMMAND ----------

val t1_b_s = spark.table("cecl_mth_bucketed_sorted")
val t2_b_s = spark.table("cecl_result_bucketed_sorted")

// COMMAND ----------

// Bucketed join. 
// No shuffle (No Exchange step in plan)...
t1_b_s.join(t2_b_s, Seq("key1")).explain()

/**
== Physical Plan ==
*(3) Project [key1#1064L, key2#1065L, value#1066, key2#1071L, value#1072]
+- *(3) SortMergeJoin [key1#1064L], [key1#1070L], Inner
   :- *(1) Sort [key1#1064L ASC NULLS FIRST], false, 0
   :  +- *(1) Project [key1#1064L, key2#1065L, value#1066]
   :     +- *(1) Filter isnotnull(key1#1064L)
   :        +- *(1) FileScan parquet default.cecl_mth_bucketed_sorted[key1#1064L,key2#1065L,value#1066] Batched: true, DataFilters: [isnotnull(key1#1064L)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/cecl_mth_bucketed_sorted], PartitionFilters: [], PushedFilters: [IsNotNull(key1)], ReadSchema: struct<key1:bigint,key2:bigint,value:double>, SelectedBucketsCount: 16 out of 16
   +- *(2) Sort [key1#1070L ASC NULLS FIRST], false, 0
      +- *(2) Project [key1#1070L, key2#1071L, value#1072]
         +- *(2) Filter isnotnull(key1#1070L)
            +- *(2) FileScan parquet default.cecl_result_bucketed_sorted[key1#1070L,key2#1071L,value#1072] Batched: true, DataFilters: [isnotnull(key1#1070L)], Format: Parquet, Location: InMemoryFileIndex[dbfs:/user/hive/warehouse/cecl_result_bucketed_sorted], PartitionFilters: [], PushedFilters: [IsNotNull(key1)], ReadSchema: struct<key1:bigint,key2:bigint,value:double>, SelectedBucketsCount: 16 out of 16
**/
