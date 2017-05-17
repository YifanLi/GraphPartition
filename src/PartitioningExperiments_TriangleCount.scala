import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import collection.mutable.HashMap
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

/*
 * to compute triangle by using graphx API for different partitionings.
 * 
 * Usage:
 * => ./bin/spark-submit --class FPPRMC ~/FPPRMC_v1.jar [partitions] [spark.executor.memory] [max.cores] [partitionStrategy] [defaultOrBlocked] [PartitonsToLoad:201;298;300]
 * e.g.
 * nohup /usr/local/spark1/spark-1.3.1-bin-hadoop1/bin/spark-submit --driver-memory 40G --class "PartitioningExperiments_TriangleCount" ~/graphx-partitioningexperimentstriangle_2.10-0.1.jar 204 45G 300 2 blockedPartitioning 201 &
 */

object PartitioningExperiments_TriangleCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FPPRMC_new_Yifan")
    conf.set("spark.ui.port","4000")
    conf.set("spark.master","spark://small1-tap1:7077")
    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.task.maxFailures", "128")
    conf.set("spark.speculation", "false")
    conf.set("spark.shuffle.manager", "SORT")
    conf.set("spark.shuffle.consolidateFiles","true")
    // Compress RDD in both memory and on-disk using the fast Snappy compression codec
    //conf.set("spark.rdd.compress", "true")
    //The codec used to compress internal data such as RDD partitions, broadcast variables and shuffle outputs. 
    //By default(snappy), Spark provides three codecs: lz4, lzf, and snappy.
    conf.set("spark.io.compression.codec","lz4")

    conf.set("spark.cores.max",args(2))
    //Number of cores to allocate for each task. 1 by default
    conf.set("spark.task.cpus","3")
    conf.set("spark.executor.memory",args(1))
    //Limit of total size of serialized results of all partitions for each Spark action (e.g. collect)
    conf.set("spark.driver.maxResultSize","5g")
    
    //fraction of spark.executor.memory which will be used for in-memory storage; to decrease it if need more memory for computation(e.g. JVM)
    conf.set("spark.storage.memoryFraction", "0.3")
    //fraction of spark.executor.memory which will be used for shuffle
    conf.set("spark.shuffle.memoryFraction", "0.4")
    //If set to "true", consolidates intermediate files created during a shuffle
    conf.set("spark.shuffle.consolidateFiles","true")
    
    conf.set("spark.eventLog.enabled", "true")
    // Help prevent FetchFailed exceptions when single node is heavily taxed
    // http://stackoverflow.com/questions/26247654/spark-fail-to-run-the-terasort-when-the-amount-of-data-gets-bigger
    conf.set("spark.core.connection.ack.wait.timeout", "6000")
    // Attempting to fix "Map output statuses were 10810591 bytes which exceeds spark.akka.frameSize (10485760 bytes)"
    conf.set("spark.akka.frameSize", "1000")
    conf.set("spark.akka.timeout","6000")
    //Size of each piece of a block in kilobytes for TorrentBroadcastFactory.
    conf.set("spark.broadcast.blockSize","10240")
    conf.set("spark.akka.threads","12")
    
    //to set the minimal parallelism to ensure only one partition for each small input file
    conf.set("spark.default.parallelism","1")

    val sc = new SparkContext(conf)
    
    //input files on HDFS
    val edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-LiveJournal1.txt"
    //val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
    val seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top200deg.txt"
    //val seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    val resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_blockpartition"
    //the directory to store the files of edge partitions
    //val parFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/edgePartitions"
    var numParName = args(5)
    //the directory to store the 201 files of edge partitions
    //val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/201_6_partitions"
    //the refined partitions from 201_6_partitions:264
    val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/"+numParName+"_6_partitions"
     

    //partitions number
    val numPartitions = args(0).toInt
    
  
  //a tag to indicate the input dataset: 
  // 1, soc-LiveJournal1.txt for 2D partitioning
  // 2, 201_6_partitions for blocked partitioning
  //val TAG = args(4).toInt

  //the total PPR walk length L for each vertex
  val wL = 200
  
  //teleportation constant
  val ct = 0.15
  
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1
  
  

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
  
  //the partitioning strategy
  //4 options in graphx: CanonicalRandomVertexCut, EdgePartition1D, EdgePartition2D, RandomVertexCut
  //val partitionStrategy = PartitionStrategy.EdgePartition2D
  val strategies = Array(PartitionStrategy.CanonicalRandomVertexCut,PartitionStrategy.EdgePartition1D, PartitionStrategy.EdgePartition2D, PartitionStrategy.RandomVertexCut)
  val numOfStra = args(3).toInt
  val partitionStrategy = strategies(numOfStra)
  
  //to indicate which dataset will be loaded
  val ind = args(4)
  
  //to construct the graph by loading an edge file on disk
  //NOTICE: for Spark 1.1, the number of partitions is set by minEdgePartitions
  var t1=0L
  var t2=0L
  var graph:Graph[Int,Int] = null
  
  if(ind.equals("defaultPartitioning")){
  println("+++++++> partition strategy:"+partitionStrategy.toString()+"<+++++++++")
  t1 = System.currentTimeMillis
  graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions, edgeStorageLevel = MAD,
     vertexStorageLevel = MAD).partitionBy(partitionStrategy)
  t2 = System.currentTimeMillis
  }else{
  /*
   * to build graph using our blocks  
   */ 
  println("-------> blocked partitioning <-------")
  t1 = System.currentTimeMillis
  graph = GraphLoader.edgeListFile(sc, edgeParFilesPathOnHDFS, false, -1, edgeStorageLevel = MAD,vertexStorageLevel = MAD)
  t2 = System.currentTimeMillis
  }
  
  println("##############################")
  println("num of edge partitions: "+graph.edges.partitions.size.toString)
  println("##############################")
     
  
  val t3 = System.currentTimeMillis
  //val iniMessage = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
  val ppr = graph.triangleCount.vertices.take(5)(1)
  val t4 = System.currentTimeMillis
  println("the time of graph computation(Pregel): "+(t4-t3))
  println("the time(ms) of graph loading: "+(t2 - t1))
  
  

  
  //to output ppr.vertices.collect
  //ppr.vertices.saveAsTextFile("/Users/yifanli/Data/PPR_temp")
  //ppr.vertices.saveAsTextFile(resultFilePathOnHDFS)
  //ppr.vertices.sample(false,0.1).saveAsTextFile(resultFilePathOnHDFS)
  println("Job done!")
    
  sc.stop()
    
  }

}