import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import collection.mutable.HashMap
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set
import java.io._

/*
 * several experiments to evaluate the performance of partitioning strategy
 * esp. the replicat factor, communication in/between partition(s)
 * 
 * Usage:
 * => ./bin/spark-submit --class PartitioningExperiments_factor ~/PartitioningExperimentsFactor_v1.jar [partitions] [spark.executor.memory] [spark.cores.max] [PartitionStrategy] [numBloPar] [blockedPartitions] [dataset: soc-LiveJournal1; soc-PokecRelationships]
 * e.g.
 * nohup /usr/local/spark1/spark-1.5.2-bin-hadoop1/bin/spark-submit --driver-memory 5G --class "PartitioningExperiments_factor" ~/graphx-verrefactor_2.10-0.1.jar 205 5G 50 2 -1 LiveJ_MerBlo200 soc-LiveJournal1 &
 */
 

object PartitioningExperiments_factor {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BlockPartitionExperimentsFactor_Yifan")
    conf.set("spark.ui.port","4000")
    conf.set("spark.master","spark://small1-tap1:7077")
    
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
    conf.set("spark.task.cpus","1")
    conf.set("spark.executor.memory",args(1))
    
    //fraction of spark.executor.memory which will be used for in-memory storage; to decrease it if need more memory for computation(e.g. JVM)
    conf.set("spark.storage.memoryFraction", "0.3")
    //fraction of spark.executor.memory which will be used for shuffle
    conf.set("spark.shuffle.memoryFraction", "0.2")
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
    //soc-LiveJournal1; soc-PokecRelationships
    var edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/"+args(6)+".txt"
    
    //val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
    val seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top200deg.txt"
    //val seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    val resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_blockpartition"
    //the directory to store the files of edge partitions
    //val parFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/edgePartitions"
    //the directory to store the files of edge partitions
    //val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/201_6_partitions"

    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number(the length of inverse P-distance)
  //var numIterations = args(3).toInt

  //the value for each seed
  val sVal = 100000d
  
  //teleportation constant
  val ct = 0.15
    
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
  
  //top-k values for each edge
  val topk = 5
  
  //the partitioning strategy
  //4 options in graphx: CanonicalRandomVertexCut, EdgePartition1D, EdgePartition2D, RandomVertexCut
  //val partitionStrategy = PartitionStrategy.EdgePartition2D
  val strategies = Array(PartitionStrategy.CanonicalRandomVertexCut,PartitionStrategy.EdgePartition1D, PartitionStrategy.EdgePartition2D, PartitionStrategy.RandomVertexCut)
  val numOfStra = args(3).toInt
  val partitionStrategy = strategies(numOfStra)
  
  //specific partition num w.r.t our blocked partitioning
  //NOTICE: -1 -> default; 
  val numBloPar = args(4).toInt
  
  //the directory on HDFS containing blocked partitions
  //the num of seed file:
  //var seedFiles = Array("17","33","65","101","151","201","16refined","32refined","64refined","100refined","150refined","298","300")
  //var parsFiles = args(5).toInt
  var parsFiles = args(5)
  //val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/"+parsFiles+"_6_partitions"
  val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/"+parsFiles
  
  println("+++++++++++++++")
  val graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions, edgeStorageLevel = MAD,
     vertexStorageLevel = MAD).partitionBy(partitionStrategy)
  println("PartitionStrategy: "+partitionStrategy.toString)
  println("numPartitons: "+numPartitions.toString)
  println("num of edge partitions: "+graph.edges.partitions.size.toString)
  
  val numReplicatedVertices = graph.edges.mapPartitions(part => Iterator(
  part.flatMap(e => Iterator(e.srcId, e.dstId)).toSet.size)).sum
  
  val numVertices = graph.vertices.count.toDouble
  println("the vertex replication factor: "+numReplicatedVertices/numVertices)
  
  println("The num of edges in each partition:")
  //graph.edges.mapPartitions(part => Iterator(part.flatMap(e => Iterator(e)).toSet.size)).foreach(x => println(x))
  
  println("+++++++++++++++")
  val graph2 = GraphLoader.edgeListFile(sc, edgeParFilesPathOnHDFS, false, numBloPar, edgeStorageLevel = MAD,
     vertexStorageLevel = MAD)
  println("PartitionStrategy: to create partitions by using default input files location")
  println("numPartitons: this parameter is set -1")
  println("num of edge partitions: "+graph2.edges.partitions.size.toString)
  
  
  val numReplicatedVertices2 = graph2.edges.mapPartitions(part => Iterator(
  part.flatMap(e => Iterator(e.srcId, e.dstId)).toSet.size)).sum
  
  val numVertices2 = graph2.vertices.count.toDouble
  println("the vertex replication factor: "+numReplicatedVertices2/numVertices2)
    
  println("The num of edges in each partition:")
  //graph2.edges.mapPartitions(part => Iterator(part.flatMap(e => Iterator(e)).toSet.size)).foreach(x => println(x))
  
  
  
  
  println("finished!")
  sc.stop
  
  
  }

}