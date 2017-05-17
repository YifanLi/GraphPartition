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
 * to analyze the time consuming of short random walks(SW) simulation on different partitionings.
 */

object PartitioningExperiments_SWtimecost {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BlockPartitionExperimentsSWtimecost_Yifan")
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
    conf.set("spark.task.cpus","1")
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
    //the directory to store the files of edge partitions
    val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/201_6_partitions"

    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number(the length of inverse P-distance)
  var numIterations = args(3).toInt
  
  //the number of samplings
  val numOfSamplings = 10

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
  val partitionStrategy = PartitionStrategy.EdgePartition2D
     
    // to randomly pick only "ONE" vertex from its neighbors
  def randomNeighbor(neighbors:Array[VertexId]) : VertexId= {
    if(neighbors.length == 0){
    	return -1
    }else{
    	return neighbors(Random.nextInt(neighbors.length))
    }
  }
  
  // to initialize the set of segments from 1st randomly choose before pregel iteration computation
  def setInit(srcID:VertexId, tag: Int, num: Int):Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])] ={
    val setOfsegs = Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]()
    if(num<1){
      return null
    }else{
      if(tag==1){
        for(i <- 1 to num){
          val buf = ArrayBuffer.empty[VertexId]
          //buf += arr(i)
          setOfsegs += (((srcID,(i,1)),buf))
        }
        //return setOfsegs
      }
      
    }
    return setOfsegs
  }
  
  // to append the randomly chosen neighbor to that segment(an array buffer)
  def appendSeg(newEndNode:VertexId, seg: ((VertexId, (Int,Int)), ArrayBuffer[VertexId])):Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])] ={
   seg._2 += newEndNode
   val setOfsegs = Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]()
   setOfsegs += seg
   return setOfsegs
  }
  // to construct a hashmap that keeps the messages will be distributed to (out-going) neighbors
  def iniWalksDistribution(neighbors:Array[VertexId]):HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]] ={
    var disHM = HashMap.empty[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]
    if(neighbors.isEmpty) return disHM
    for(e <- neighbors){
      disHM += (e -> Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])])
    }
    return disHM
  }

  // to build graph using default partitioning strategy
  
  val t1 = System.currentTimeMillis
  val graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions, edgeStorageLevel = MAD,
     vertexStorageLevel = MAD).partitionBy(partitionStrategy)
  val t2 = System.currentTimeMillis
  
  /*
   * to build graph using our blocks  
   */ 
  /*
  val t1 = System.currentTimeMillis
  val graph = GraphLoader.edgeListFile(sc, edgeParFilesPathOnHDFS, false, -1, edgeStorageLevel = MAD,vertexStorageLevel = MAD)
  val t2 = System.currentTimeMillis
  */
  //notice that here only one neighbor is picked for each sampling.
  val firstRondomedVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, (setInit(id,1,numOfSamplings), (a, iniWalksDistribution(a))))}.persist(MAD)
  // val firstRondomedVertexRDD = ~.persist(MAD).coalesce(numPartitions,true)

  /*
    Warning: the partitioning of edge would be changed after construct the new graph as following!!!
     */
  //var initialGraph = Graph(firstRondomedVertexRDD, graph.edges)
  var initialGraph = Graph(firstRondomedVertexRDD, graph.edges,null,edgeStorageLevel = MAD, vertexStorageLevel = MAD)

  
  /*
   * <-------Segments Building-------->
   * 1) w.r.t the algorithm proposed by Cedric
   * 2) notice that the samplings number and segment length are both pre-set.
   * 3) the vertex attribute(VD):
   *    (set of ((srcVId,(PathId,SegmentId)), segment to current vertex), array of out-going neighbors)
   */
  val iniMessage = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
  iniMessage += (((-10L,(0,0)),ArrayBuffer.empty[VertexId]))
  
  def vertexProgram(id: VertexId, attr: (Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])],(Array[VertexId],HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]])), msgSum: Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]):(Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])],(Array[VertexId],HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]))={
    
      if(attr._2._1.length == 0){
        if(msgSum.size==1 && msgSum.head._1._1 == -10L){
          return attr
        }else{
        	return (attr._1++msgSum, (attr._2._1,HashMap.empty[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]))
        }
      }else{
        if(msgSum.size==1 && msgSum.head._1._1 == -10L){
        	attr._1.foreach(x => {
        		val node = randomNeighbor(attr._2._1)
        		attr._2._2 += (node -> (attr._2._2.apply(node)++appendSeg(node,x)))
        	}
        	)
        	return(Set.empty,attr._2)
        }else{
          attr._2._2.foreach(e => {
            attr._2._2 += (e._1 -> Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])])
          }
            )
          msgSum.foreach(y => {
            val tbuf = new ArrayBuffer[VertexId]()
            tbuf ++= y._2
            val ty = (y._1,tbuf)
            val nd = randomNeighbor(attr._2._1)
            attr._2._2 += (nd -> (attr._2._2.apply(nd)++appendSeg(nd,ty)))
          }
          )
          
          return (msgSum, attr._2)
      
        }
        
      }
    
    
  }

  def sendMessage(edge: EdgeTriplet[(Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])],(Array[VertexId],HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]])), Int]): Iterator[(VertexId, Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])])]={
    val dis = edge.srcAttr._2._2.apply(edge.dstId)
    //println("@@@@@@@@@@@@@@@@@@@@@@@")
    //println(edge.srcId)
    //println(edge.dstId)
    //println("-----")
    //println(dis)
    //println("$$$$$$$$$$$$$$$$$$$$$$$")
    if(dis.isEmpty){
      Iterator.empty
    }else{
      Iterator((edge.dstId,dis))
    }
  }
  
  def messageCombiner(a:Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])], b:Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]):Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])] = {
	//val c = new HashMap[VertexId, Double]()	
    return a ++ b
  }
  

  //ppr.vertices.collect
  //ppr.vertices.saveAsTextFile(resultFilePath)
  println("the time(ms) of graph loading: "+(t2 - t1))
  
  val t3 = System.currentTimeMillis
  //val iniMessage = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  val t4 = System.currentTimeMillis
  println("the time of graph computation(Pregel): "+(t4-t3))
  
  
  println("Finished!")
  sc.stop()
  
  
  
  }

}