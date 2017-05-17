import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import collection.mutable.HashMap
//import scala.collection.mutable.ArrayBuffer

/*
 * (Notice: some counters were added in this program to measure the messages size(num of hashmap elements) of transit)
 * 
 * to computer personalized page rank:
 * 1) pre-computing is finished in this program: the computation of hub vectors(inverse P-distance)
 * 2) need to do beyond this program: assembly of these hub vectors by using a corresponding preference vector
 */
class PersonalizedPR_counters {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  val edgesFile = "/Users/yifanli/Data/data_test.txt"
  val resultFilePath = "/Users/yifanli/Data/PPR"

  //partitions number
  val numPartitions = 8
  
  //iterations number:
  //BFS depths for each iteration: 2, 4, 8, ...
  //note that the initial depth(pre-iterations) is 1
  val numIterations = 1

  //BFS depth, corresponding to numIterations
  //numIterations = log2(depthOfBFS)
  val depthOfBFS = 4
  
  //teleportation constant
  val ct = 0.15
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1
  
  //a function to combine the vertex's hashmap with those of its neighbors
  def hmMerge(dstID:VertexId, dstHM:HashMap[VertexId, Double], srcAtt_1: Int) : HashMap[VertexId, Double] = {
    //val srcOutDegree = srcHM.apply(-1)
    //val lenToDst = srcHM.apply(dstID)(0)
    //val scoreToDst = srcHM.apply(dstID)(1)
    val tempHM = new HashMap[VertexId, Double]()
    val iniScore = ct/srcAtt_1
    dstHM.foreach(p =>{
      tempHM += (p._1 -> p._2.*(iniScore))
    }
      )
    //here, cycle is not considered, otherwise
    //dstHM += (dstID -> Array(lenToDst,scoreToDst*dstHM.apply(dstID)(1)))
    tempHM += (dstID -> iniScore)
    println("ddddd")
    //to update the counter of this message which will be transit to srcVertex 
    tempHM.update(-100L, tempHM.size)
   
    return tempHM
  }
  
  // to construct the initial hashmap of each vertex before PPR computation
  
  def initialHashMap(arr:Array[VertexId]):(Int,HashMap[VertexId, Double]) = {
    val hm = new HashMap[VertexId, Double]()
    for(id <- arr){
      hm += (id -> ct/arr.length)
    }
    
    //val outD:VertexId = -1
    //hm += (outD -> Array(arr.length.toDouble,0))
    //to add a counter
    hm.update(-100L, 0.0)
    return (arr.length,hm)
  }
  
  
  //to construct the graph by loading an edge file on disk
  val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
  
  //to add a hashmap to each vertex(for now, it is the only vertex-property)
  //{(path_length : distance_score), ...}
  //val initialGraph: Graph[HashMap[VertexId, HashMap[Int, Long]], Int] = graph.mapVertices{ (id, _) => HashMap[VertexId, HashMap[Int, Long]](id ->HashMap[Int, Long](0 -> 10L))}
  val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}.persist(StorageLevel.MEMORY_AND_DISK)
  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  //var initialGraph = Graph(initialVertexRDD, graph.edges).persist(StorageLevel.MEMORY_ONLY)
  //val edgeStorageLevel=StorageLevel.MEMORY_AND_DISK
  var initialGraph = Graph(initialVertexRDD, graph.edges)
  
  /*
   * to construct those 3 functions for GraphX version of Pregel
   * vertexProgram(id: VertexId, attr: HashMap[VertexId, Array[Double]], msgSum: HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]]
   * sendMessage(edge: EdgeTriplet[HashMap[VertexId, Array[Double]], HashMap[VertexId, Array[Double]]])
   * messageCombiner(a:HashMap[VertexId, Array[Double]], b:HashMap[VertexId, Array[Double]])
   * 
   */
  def vertexProgram(id: VertexId, attr: (Int,HashMap[VertexId, Double]), msgSum: HashMap[VertexId, Double]):(Int,HashMap[VertexId, Double])={
    if(msgSum.isEmpty){
      return attr
    }
    else
      println("aaaaa")
      //return msgSum += (outD -> attr.apply(outD))
      msgSum.update(-100L, attr._2.getOrElse(-100L, 0.0)+msgSum.getOrElse(-100L, 0.0))
      return (attr._1, msgSum)
  }
  def sendMessage(edge: EdgeTriplet[(Int,HashMap[VertexId, Double]), Int]) =
    Iterator((edge.srcId, hmMerge(edge.dstId,edge.dstAttr._2,edge.srcAttr._1)))
  def messageCombiner(a:HashMap[VertexId, Double], b:HashMap[VertexId, Double]):HashMap[VertexId, Double] = {
	val c = new HashMap[VertexId, Double]()
	c ++=a
	b.foreach(p => {
		if(a.contains(p._1))
		  c += (p._1 -> a.apply(p._1).+(p._2))
		else
		  c += p
		}
	)
	
    return c
  }
    
  val iniMessage = new HashMap[VertexId, Double]()
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, messageCombiner)
  
  // to get the number of all hashmap elements which has been transfered within messages
  var numHashMapEleInMessages = 0.0
  //val ab = ArrayBuffer[Double](0.0)
  ppr.vertices.toArray.foreach( p => {
    numHashMapEleInMessages = numHashMapEleInMessages + p._2._2.getOrElse(-100L, 0.0)
  	}
  )
  println("numHashMapEleInMessages:" + numHashMapEleInMessages)
  
  //ppr.vertices.foreach(p => println(p._2._2.getOrElse(-100L, 0.0)))
  
  //ppr.vertices.collect
  ppr.vertices.saveAsTextFile(resultFilePath)
  
  println("finished!")
  
}