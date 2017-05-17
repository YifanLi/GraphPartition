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
 * some methods to analyze graph, e.g. topology
 * esp. to select the needed seeds from graph for partitioning
 *
 * 
 * Usage:
 * => ./bin/spark-submit --class BlockedPartition ~/BlockPartition_v1.jar [partitions] [spark.executor.memory] [spark.cores.max] [iterations] [maxSeedNum] [Pokec] [ConsideringNeighborOrNot:NoNeighbor or any others] [CandidatesOfTopK]
 * e.g.
 * nohup /usr/local/spark1/spark-1.5.2-bin-hadoop1/bin/spark-submit --driver-memory 30G --class "methodsToAnalyzeGraph" ~/graphx-blockedpartition-seedselection_2.10-0.1.jar 300 20G 150 3 2000 Pokec NoNeighbor 1000000 &
 * 
 */

object methodsToAnalyzeGraph {
  def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("ToAnalyzeGraph_Yifan")
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
    
    //fraction of spark.executor.memory which will be used for in-memory storage; to decrease it if need more memory for computation(e.g. JVM)
    conf.set("spark.storage.memoryFraction", "0.2")
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

    val sc = new SparkContext(conf)
    
    //input files on HDFS
    var edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-LiveJournal1.txt"
    //val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
    //var seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top200deg.txt"
    //val seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    //var resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_blockpartition"
    //the directory to store the files of edge partitions
    //var parFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/edgePartitions"
    var prefix = ""
    if(args(5).equals("Pokec")){
      edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-PokecRelationships.txt"
      prefix = args(5)
    }
    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number(the length of inverse P-distance)
  var numIterations = args(3).toInt

    //storage levels
    val MADS = StorageLevel.MEMORY_AND_DISK_SER
    val MAD = StorageLevel.MEMORY_AND_DISK
    
    // the number of candidates for TopK
    val TK = args(7).toInt
    
    // the max number of seeds
    val maxSeedNum = args(4).toInt
    
    val graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions, edgeStorageLevel = MAD,
     vertexStorageLevel = MAD).partitionBy(PartitionStrategy.EdgePartition2D)
     
    /*
     * to create a vertices RDD of marks:
     * 0: not has been marked
     * 1: marked, means this vertex can not be used as seed.
     */
     //val markVerRdd = graph.vertices.map((vid, data) => (vid, 0))
     
     /*
      * to store the marked vertices using a Set
      */
     var markedVer = Set[VertexId]()
     
    /*
     * to get all the out-going neighbors ids
     */ 
     val allOutNeigs = graph.collectNeighborIds(EdgeDirection.Out)
     
     /*
     * to get all the in-coming neighbors ids
     */ 
     val allInNeigs = graph.collectNeighborIds(EdgeDirection.In)
    
    /*
     * to get the out-going degree of each vertex
     */
     //val odVerRDD = graph.outDegrees.persist(MAD).coalesce(numPartitions,true)

     /*
      * to get the in-coming degree of each vertex
      */
     //val idVerRDD = graph.inDegrees.persist(MAD).coalesce(numPartitions,true)
     
     /*
      * to get the degree of each of each vertex
      */
     val dVerRDD = graph.degrees.persist(MAD).coalesce(numPartitions,true)
     
     /*
      * to get the top-K degree vertices from graph
      */
     val topKArray = dVerRDD.map{case(id,deg) => (deg,id)}.top(TK)
     
     /*
      * to get the top-K out-degree vertices from graph
      */
     //val topKodArray = odVerRDD.map{case(id,deg) => (deg,id)}.top(TK)
     
     if(args(6).equals("NoNeighbor")){
     /*
      * to write Top-TK out-degree vertices(considering neighbors) into local file
      */
     val writer = new PrintWriter(new File("Top"+prefix+maxSeedNum.toString+"deg.txt" ))
     //writer.write("the time(ms) of graph loading: "+(t2 - t1).toString)
     //writer.write("the time(ms) of pregel computation: "+(t4 - t3).toString)
     
     var i =1
     for(x <- topKArray; if i <= maxSeedNum){
       if(!markedVer.contains(x._2)){
    	   writer.write(x._2.toString +"\t"+"100000.0"+ "\n")
    	   val outNeigIds = allOutNeigs.filter{case(id, _) => id==x._2}.first._2
    	   val inNeigIds = allInNeigs.filter{case(id, _) => id==x._2}.first._2
    	   for(y <- outNeigIds){
    	     markedVer += y
    	   }
    	   for(z <- inNeigIds){
    	     markedVer += z
    	   }
    	   
    	   i = i+1
        }
       }
     writer.close()
     
     }else{
       //Without consideration of neighbors
       val writer = new PrintWriter(new File("Top"+prefix+"_keepNeigh_"+maxSeedNum.toString+"deg.txt" ))
       val topKArray = dVerRDD.map{case(id,deg) => (deg,id)}.top(maxSeedNum)
       for(x <- topKArray){
         writer.write(x._2.toString +"\t"+"100000.0"+ "\n")
       }
       writer.close()
     }
     
     
     println("Job done!")
     sc.stop()
     
     
  }

}