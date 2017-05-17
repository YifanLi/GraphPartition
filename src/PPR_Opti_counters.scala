import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import collection.mutable.HashMap

/*
 * to computer personalized page rank with optimization on messages transferring between nodes
 * (Basic Idea: only those "incremental messages" in this superstep computation will be transfered)
 * 1) pre-computing is finished in this program: the computation of hub vectors(inverse P-distance)
 * 2) need to do beyond this program: assembly of these hub vectors by using a corresponding preference vector
 * 
 * Notice: some counters were added in this program to measure the messages size(num of hashmap elements) in transit
 */
class PPR_Opti_counters {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR").setMaster("localhost")
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
  
  //the Delta(incrementation) to decide if a message should be transfered
  val Delta = 0.001
  
  //a function to combine the vertex's hashmap with those of its neighbors
  def hmMerge(dstID:VertexId, dstHM:HashMap[VertexId, Array[Double]], srcAtt_1: Int) : HashMap[VertexId, Double] = {
    //val srcOutDegree = srcHM.apply(-1)
    //val lenToDst = srcHM.apply(dstID)(0)
    //val scoreToDst = srcHM.apply(dstID)(1)
    val tempHM = new HashMap[VertexId, Double]()
    //println("~~~~~~~~&&&&&&&&&&&~~~~~~~~")
    //println(dstHM)
    //println("~~~~~~~~&&&&&&&&&&&~~~~~~~~")
    val iniScore = ct/srcAtt_1
    dstHM -= (-100L)
    dstHM.foreach(p =>{
      if(p._2(1) > Delta){
    	  tempHM += (p._1 -> (p._2(1))*(iniScore))
      }
    }
      )
    //here, cycle is not considered, otherwise
    //dstHM += (dstID -> Array(lenToDst,scoreToDst*dstHM.apply(dstID)(1)))
      
    //tempHM += (dstID -> iniScore)
    println("ddddd")
    tempHM.update(-100L, tempHM.size)
   
    return tempHM
  }
  
  
  /*
   * to construct the initial hashmap of each vertex before PPR computation
   * Notice:
   * - the 1st element in Array[Double] is to indicate the score
   * - the 2nd element is the incrementation (Delta), to indicate whether the score had a "big" change(w.r.t that Delta) after this superstep
   */
  
  def initialHashMap(arr:Array[VertexId]):(Int,HashMap[VertexId, Array[Double]]) = {
    val hm = new HashMap[VertexId, Array[Double]]()
    val numOutEdges = arr.length
    for(id <- arr){
    	  hm += (id -> Array(ct/numOutEdges,ct/numOutEdges))
    }
    
    //val outD:VertexId = -1
    //hm += (outD -> Array(arr.length.toDouble,0))
    hm.update(-100L, Array[Double](0.0,0.0))
    return (numOutEdges,hm)
  }
  
  
  //to construct the graph by loading an edge file on disk
  val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
  
  //to add a hashmap to each vertex(for now, it is the only vertex-property)
  val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}.persist(StorageLevel.MEMORY_AND_DISK)
  var initialGraph = Graph(initialVertexRDD, graph.edges)
  
  //val graph2 = GraphImpl(graph.edges, initialVertexRDD, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
  
  /*
   * to construct those 3 functions for GraphX version of Pregel
   * vertexProgram(id: VertexId, attr: HashMap[VertexId, Array[Double]], msgSum: HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]]
   * sendMessage(edge: EdgeTriplet[HashMap[VertexId, Array[Double]], HashMap[VertexId, Array[Double]]])
   * messageCombiner(a:HashMap[VertexId, Array[Double]], b:HashMap[VertexId, Array[Double]])
   * 
   */
  def vertexProgram(id: VertexId, attr: (Int,HashMap[VertexId, Array[Double]]), msgSum: HashMap[VertexId, Double]):(Int,HashMap[VertexId, Array[Double]])={
	//println(msgSum)
	//println("***********************************")
    if(msgSum.size.equals(1) && msgSum.contains(-1L)){
      //println(msgSum)
      return attr
    }
    
    //to set the incrementations to zero for each target-node in hashmap
    attr._2.foreach(p => {
      p._2(1) = 0.0
    }
      )
    if(msgSum.isEmpty){
      return attr
    }
    else
      //return msgSum += (outD -> attr.apply(outD))
      msgSum.foreach(p => {
        if(attr._2.contains(p._1)){
          attr._2.update(p._1, Array(attr._2.apply(p._1)(0)+p._2, p._2))
        }
        else{
          attr._2.update(p._1, Array(p._2, p._2))
        }
      }
        )
      return attr
  }
  def sendMessage(edge: EdgeTriplet[(Int,HashMap[VertexId, Array[Double]]), Int]) =
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
    
  //val iniMessage = new HashMap[VertexId, Double]()
  val iniMessage = HashMap(-1L -> 0.001)
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, messageCombiner)
  
  // to get the number of all hashmap elements which has been transfered within messages
  var numHashMapEleInMessages = 0.0
  //val ab = ArrayBuffer[Double](0.0)
  ppr.vertices.toArray.foreach( p => {
    numHashMapEleInMessages = numHashMapEleInMessages + p._2._2.getOrElse(-100L,Array(0.0,0.0))(0)
  	}
  )
  
  println("numHashMapEleInMessages:" + numHashMapEleInMessages)
  
  
  //ppr.vertices.collect
  ppr.vertices.saveAsTextFile(resultFilePath)
  
  println("finished!")
  
}