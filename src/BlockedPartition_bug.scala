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
 * to partition the graph basing on blocks.
 * 1) vertex-cut(edges partition)
 * 2) seed vertices(e.g. Top300outdeg)
 * 3) the value of seed will be propagated evenly to its neighbors for numIterations iterations
 * 
 * Usage:
 * => ./bin/spark-submit --class BlockedPartition ~/BlockPartition_v1.jar [partitions] [spark.executor.memory]
 * 
 */

object BlockedPartition_bug {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BlockPartition_Yifan")
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

    conf.set("spark.cores.max","200")
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
    //val edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-LiveJournal1.txt"
    val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
    //val seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top300outdeg.txt"
    val seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    val resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_blockpartition"
     

    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number:
  var numIterations = 5

  //the value for each seed
  val sVal = 100000d
  
  //teleportation constant
  val ct = 0.15
    
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
  


//a function to divide the values in vertex's hashmap by its out-degree
  def hmCal(srcAtt: (Int,HashMap[VertexId,Double])) : HashMap[VertexId, Double] = {
    val hm = new HashMap[VertexId, Double]()
    //hm ++= srcAtt._2
    srcAtt._2.foreach(p =>{
      //tempHM += (p._1 -> p._2.*(iniScore))
      //println("++++++++++++++++++++")
      //println(p._2)
      hm += ((p._1 -> p._2*(1/srcAtt._1.toDouble)))
      //println("*******************")
      //println(p._2*(1/srcAtt._1.toDouble))
      //println("*******************")
      //println(srcAtt._2.apply(p._1))
    }
      )
    //println("ddddd")
   
    return hm
  }
  
  
  /*
   * to construct the initial hashmap of each vertex before PPR computation
   * this hashmap[VertexId, Double] is used to store those rank values received from every hub
   * [Hub_VertexId -> rank]
   */
  
  def initialHashMap(vid:VertexId, v: Double):HashMap[VertexId, Double] = {
    val hm = new HashMap[VertexId, Double]()
    if(v>0){
      hm +=(vid -> v)
    }
    
    return hm
  }
  
  
  //to construct the graph by loading an edge file on disk
  val graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, numEdgePartitions = numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
  
  // to load the hub vertices file to construct a hub vertices RDD[(VertexId, Double)]
  val hubRDD: RDD[(VertexId, Double)] = sc.textFile(seedsFileOnHDFS).map(line => (line.trim.toLong, sVal))
  
  //val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}.persist(StorageLevel.MEMORY_AND_DISK)
  
  // to get the out degrees of each vertex
  val outDeg = graph.outDegrees
  //val verRDD3 = outDeg.leftJoin(hubRDD)((id, a, b) => (a, b.getOrElse(0.0))).map{case(id,ab) => (id, (ab._1,(initialHashMap(id,ab._2),initialHashMap(id,ab._2))))}.persist(StorageLevel.MEMORY_AND_DISK).coalesce(numPartitions,true)
  val verRDD3 = outDeg.leftJoin(hubRDD)((id, a, b) => (a, b.getOrElse(0.0))).map{case(id,ab) => (id, (ab._1,(initialHashMap(id,ab._2),initialHashMap(id,ab._2))))}.persist(StorageLevel.MEMORY_AND_DISK)
  //to construct the vertices RDD by joining with hubs preference values
  //val verRDD1 = graph.vertices.leftJoin(hubRDD)((id, a, b) => a+b.getOrElse(0.0)-1.0).map{case(id:VertexId,v:Double) => (id, (initialHashMap(id,v), initialHashMap(id,v)))}.persist(StorageLevel.MEMORY_AND_DISK).coalesce(numPartitions,true)

  //val verRDD = verRDD1.leftJoin(outDeg)((id,hh,d) => (d,hh)).persist(StorageLevel.MEMORY_AND_DISK).coalesce(numPartitions,true)

  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  //var initialGraph = Graph(initialVertexRDD, graph.edges).persist(StorageLevel.MEMORY_ONLY)
  //val edgeStorageLevel=StorageLevel.MEMORY_AND_DISK
  //var initialGraph = Graph(verRDD, graph.edges, null,edgeStorageLevel = MAD, vertexStorageLevel = MAD).outerJoinVertices(outDeg){(vid, hmc, optDeg) => (optDeg.getOrElse(0), hmc)}
  var initialGraph = Graph(verRDD3, graph.edges, null,edgeStorageLevel = MAD, vertexStorageLevel = MAD)
  /*
   * to construct those 3 functions for GraphX version of Pregel
   * vertexProgram(id: VertexId, attr: HashMap[VertexId, Array[Double]], msgSum: HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]]
   * sendMessage(edge: EdgeTriplet[HashMap[VertexId, Array[Double]], HashMap[VertexId, Array[Double]]])
   * messageCombiner(a:HashMap[VertexId, Array[Double]], b:HashMap[VertexId, Array[Double]])
   * 
   * Notice:
   * the formula of inverse P-distance is r(q) = c* (P[t]*(1-c)^l(t))
   * Thus, the "c*" will be ignored from calculation since it has no effect on the final rank result.
   */
  def vertexProgram(id: VertexId, attr: (Int,(HashMap[VertexId, Double],HashMap[VertexId, Double])), msgSum: HashMap[VertexId, Double]):(Int,(HashMap[VertexId, Double],HashMap[VertexId, Double]))={
    val inim:VertexId = -1
    val msg = new HashMap[VertexId, Double]()
    println("5656565656")
	msg ++= msgSum
	println("7878787878")
    
    
    if(msg.contains(inim)){
      println("-111111111111")
      println("srcID: "+ id.toString)
      //println(attr._2)
      //attr._2._2.foreach(p => println(p))
      println("msgSum.size: "+msg.size.toString)
      //println(msgSum)
      //msg.foreach(p => println(p))
      println("^^^^^^^^")
      return attr
    }else{
    
    //val ta = new HashMap[VertexId, Double]()
    //println("12121212121")
	//ta ++=attr._2._1
	//println("343434343434")
	//val temp = new HashMap[VertexId, Double]()
	//temp ++=ta

    //if(msgSum.size == 0){return attr}
    println("aaaaa")
    println("srcID: "+ id.toString)
    println("msgSum.size: "+msg.size.toString)
    //msgSum.foreach(p => println())
    println("------1")
    //println(msgSum)
    //msg.foreach(p => println(p))
    println("------2")
    
    msg.foreach(p => {
      println(p._1.toString + "->" + p._2.toString)
      if(attr._2._1.contains(p._1)){
        println("attr._2._1.apply(p._1): "+attr._2._1.apply(p._1))
        //ta.update(p._1, p._2 + attr._2._1.apply(p._1))
        attr._2._1 += (p._1 -> attr._2._1.apply(p._1).+(p._2))
        println("@@@@@")
      }else{
        println("$$$$$")
        attr._2._1 += ((p._1 -> p._2))
        println("~~~~~~")
        //ta.update(p._1, p._2)
      }
      //println("attr._2._1.getOrElse: "+attr._2._1.getOrElse(p._1, 0.0).toString)
      //attr._2._1.update(p._1, p._2 + attr._2._1.getOrElse(p._1, 0.0))
      //attr._2._1.update(1L,2d)
    	//for accurate rank value:
    	//attr._2._1.update(p._1, p._2 + attr._2._1.getOrElse(p._1, 0.0)*ct)
    }
      )
    return (attr._1, (attr._2._1,msg))
    }
  }
  
  def sendMessage(edge: EdgeTriplet[(Int,(HashMap[VertexId, Double],HashMap[VertexId, Double])), Int]) = {
    
    if(edge.srcAttr._2._2.isEmpty){
      Iterator.empty
    }else{
      val hc = new HashMap[VertexId, Double]()
      val tmp = new HashMap[VertexId, Double]()
      println("1919191919")
      tmp ++= edge.srcAttr._2._2
      println("2929292929")
      hc ++= hmCal(edge.srcAttr._1,tmp)
      println("!!!!!!!!!!!")
      println("targetID: "+edge.dstId.toString)
      println(hc)
      Iterator((edge.dstId, hc))
    }
  }
  def messageCombiner(a:HashMap[VertexId, Double], b:HashMap[VertexId, Double]):HashMap[VertexId, Double] = {
	//val ta = new HashMap[VertexId, Double]()
	//ta ++=a
	val tb = new HashMap[VertexId, Double]()
	println("4444444")
	tb ++=b
	println("5555555")
	tb.foreach(p => {
		if(a.contains(p._1))
		  a += (p._1 -> a.apply(p._1).+(p._2))
		else
		  a += ((p._1 -> p._2))
		}
	)
	println("8888888")
    return a
  }
    
  val iniMessage = HashMap[VertexId, Double](-1L -> 0.0)
  //val iniMessage = new HashMap[VertexId, Double]()
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  
  val vertices = ppr.vertices.map{case(id: VertexId, attr: (Int,(HashMap[VertexId, Double],HashMap[VertexId, Double]))) => (id, attr._2._1)}
  //vertices.collect
  vertices.saveAsTextFile(resultFilePathOnHDFS)
  
  //to count the useful vertices
  val usefulVertices = vertices.filter{case(id:VertexId, attr:HashMap[VertexId, Double]) => attr.size >0}
  println("The num of useful vertices: " + usefulVertices.count.toString)
  
  
  println("finished!")
  
    
    
    
  }

}