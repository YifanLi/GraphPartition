import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.lib.ShortestPaths
import collection.mutable.HashMap
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

/*
 * to compute Shortest Path for each vertex, regarding to the landmarks(a sequence of vertices), by using graphx API for different partitionings.
 * 
 * Usage:
 * => ./bin/spark-submit --class FPPRMC ~/FPPRMC_v1.jar [partitions] [spark.executor.memory] [max.cores] [partitionStrategy] [defaultOrBlocked] [PartitonsToLoad:201;298;300;Pokec200refined;LiveJournal1_PowerGraph200] [number of landmarks]
 * e.g.
 * nohup /usr/local/spark/spark-1.6.2-bin-hadoop1/bin/spark-submit --driver-memory 40G --class "PartitioningExperiments_ShortestPaths" ~/graphx-partitioningexperimentsshortest_2.10-0.1.jar 204 45G 300 2 blockedPartitioning LiveJ_fixed200 5 &
 */

object PartitioningExperiments_ShortestPaths {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FullyRWs_Yifan")
    conf.set("spark.ui.port","4000")
    conf.set("spark.master","spark://small1:7077")
    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //to set the maximum allowable size of Kryo serialization buffer; This must be larger than any object you attempt to serialize
    conf.set("spark.kryoserializer.buffer.max.mb", "1024")
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
    
    //fraction of spark.executor.memory which will be used for in-memory storage; to decrease it if need more memory for computation(e.g. JVM)
    conf.set("spark.storage.memoryFraction", "0.3")
    //fraction of spark.executor.memory which will be used for shuffle
    conf.set("spark.shuffle.memoryFraction", "0.4")
    //If set to "true", consolidates intermediate files created during a shuffle
    conf.set("spark.shuffle.consolidateFiles","true")
    
    conf.set("spark.eventLog.enabled", "false")
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
    
    var numParName = args(5)
    
    //input files on HDFS
    var edgesFileOnHDFS = "hdfs://small1:54310/user/yifan/soc-PokecRelationships.txt"
    if(numParName.startsWith("Pokec")){
    	edgesFileOnHDFS = "hdfs://small1:54310/user/yifan/soc-PokecRelationships.txt"
    }else{
    	edgesFileOnHDFS = "hdfs://small1:54310/user/yifan/soc-LiveJournal1.txt"
    }
    //val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //val landmarksFileOnHDFS = "hdfs://small1:54310/user/yifan/landmarks.txt"
    val seedsFileOnHDFS = "hdfs://small1:54310/user/yifan/Top200deg.txt"
    //val seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    val resultFilePathOnHDFS = "hdfs://small1:54310/user/yifan/soc_blockpartition"
    //the directory to store the files of edge partitions
    //val parFilePathOnHDFS = "hdfs://small1:54310/user/yifan/edgePartitions"
    
    //the directory to store the 201 files of edge partitions
    //val edgeParFilesPathOnHDFS = "hdfs://small1:54310/user/yifan/201_6_partitions"
    //the refined partitions from 201_6_partitions:264
    val edgeParFilesPathOnHDFS = "hdfs://small1:54310/user/yifan/"+numParName
     

    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number:
  //val numIterations = args(3).toInt
  
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
  
  //the number of samplings
  //val numOfSamplings = args(6).toInt
  
  //the length of a segment
  val lenOfSeg = 5

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
  
  //the sequence of candidate landmarks, 20 ones
  val clms:Seq[VertexId] = Seq(1, 10, 20, 100, 150, 300, 1000, 1500, 3000, 5000, 10000, 15000, 17000, 50000, 150000, 200000, 500000, 600000, 1000000, 1200000)
  
  //the number of landmarks pre-set
  val numLan = args(6).toInt
  //the landmarks
  var landmarks:Seq[VertexId] = Seq()
  
  if(numLan > clms.size){
    println("the pre-set number of landmarks is too large!")
  }else{
    landmarks = clms.take(numLan)
  }
  
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
  
  
  //val iniMessage = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
  //val ppr = graph.triangleCount.vertices.take(5)(1)
  if(lanmarks.size > 0){
    val t3 = System.currentTimeMillis
    val resGraph = ShortestPaths.run(graph, landmarks)
    val t4 = System.currentTimeMillis
    println("the time of graph computation(Pregel): "+(t4-t3))
    println("the time(ms) of graph loading: "+(t2 - t1))
    
  }else{
    println("No landmarks!")
  }
  
  
  

  
  //to output ppr.vertices.collect
  //ppr.vertices.saveAsTextFile("/Users/yifanli/Data/PPR_temp")
  //ppr.vertices.saveAsTextFile(resultFilePathOnHDFS)
  //ppr.vertices.sample(false,0.1).saveAsTextFile(resultFilePathOnHDFS)
  println("Job done!")
  
  
  
  
  
  
  }

}