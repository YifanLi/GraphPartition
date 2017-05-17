import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import collection.mutable.HashMap

/*
 * to computer personalized page rank
 */
class ppr_new {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  val edgesFile = "/local/liy/data/soc-LiveJournal1.txt"

  //partitions number
  val numPartitions = 8
  
  //iterations number:
  //BFS depths for each iteration: 2, 4, 8, ...
  //note that the initial depth(pre-iterations) is 1
  val numIterations = 2

  //BFS depth, corresponding to numIterations
  //numIterations = log2(depthOfBFS)
  val depthOfBFS = 4
  
  //teleportation constant
  val ct = 0.15
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1
  
  //a function to combine the vertex's hashmap with those of its neighbors
  def hmMerge(dstID:VertexId, dstHM:HashMap[VertexId, Array[Double]], srcHM: HashMap[VertexId, Array[Double]]) : HashMap[VertexId, Array[Double]] = {
    //val srcOutDegree = srcHM.apply(-1)
    //val lenToDst = srcHM.apply(dstID)(0)
    //val scoreToDst = srcHM.apply(dstID)(1)
    println("ccccc")
    val iniScore = ct/srcHM.apply(outD)(0)
    dstHM -=(outD)
    dstHM.foreach(p =>{
      //p._2(0) += lenToDst
      //p._2(1) *= scoreToDst
      p._2(0) += 1
      p._2(1) *= iniScore
    }
      )
    //here, cycle is not considered, otherwise
    //dstHM += (dstID -> Array(lenToDst,scoreToDst*dstHM.apply(dstID)(1)))
    dstHM += (dstID -> Array(1,iniScore))
    println("ddddd")
    dstHM += (outD -> srcHM.apply(outD))
    
    return dstHM
  }
  
  // to construct the initial hashmap of each vertex before PPR computation
  // Array[VertexId] => HashMap[VertexId, Array[Double](2)] 
  // the 1st element of DoubleArray is the max length of paths from src_vertex to target_vertex in previous steps
  // the 2nd one is the scores(inverse P-distance)
  
  def initialHashMap(arr:Array[VertexId]):HashMap[VertexId, Array[Double]] = {
    val hm = new HashMap[VertexId, Array[Double]]()
    for(id <- arr){
      hm += (id -> Array(1,ct/arr.length))
    }
    
    //val outD:VertexId = -1
    hm += (outD -> Array(arr.length.toDouble,0))
    return hm
  }
  
  
  //to construct the graph by loading an edge file on disk
  val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions).partitionBy(PartitionStrategy.EdgePartition2D)
  
  //to add a hashmap to each vertex(for now, it is the only vertex-property)
  //{(path_length : distance_score), ...}
  //val initialGraph: Graph[HashMap[VertexId, HashMap[Int, Long]], Int] = graph.mapVertices{ (id, _) => HashMap[VertexId, HashMap[Int, Long]](id ->HashMap[Int, Long](0 -> 10L))}
  val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}
  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  var initialGraph = Graph(initialVertexRDD, graph.edges).cache
  
  /*
   * to construct those 3 functions for GraphX version of Pregel
   * vertexProgram(id: VertexId, attr: HashMap[VertexId, Array[Double]], msgSum: HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]]
   * sendMessage(edge: EdgeTriplet[HashMap[VertexId, Array[Double]], HashMap[VertexId, Array[Double]]])
   * messageCombiner(a:HashMap[VertexId, Array[Double]], b:HashMap[VertexId, Array[Double]])
   * 
   */
  def vertexProgram(id: VertexId, attr: HashMap[VertexId, Array[Double]], msgSum: HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]]={
    if(msgSum.isEmpty){
      return attr
    }
    else
      println("aaaaa")
      return msgSum += (outD -> attr.apply(outD))
  }
  def sendMessage(edge: EdgeTriplet[HashMap[VertexId, Array[Double]], Int]) =
    Iterator((edge.srcId, hmMerge(edge.dstId,edge.dstAttr,edge.srcAttr)))
  def messageCombiner(a:HashMap[VertexId, Array[Double]], b:HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]] = {
	  val outDeg = a.apply(outD)
	  println("++++++++++++")
      if(a.contains(3)){println(a.apply(3)(0),a.apply(3)(1))}
	  println("@@@@@@@@@@@")
      if(b.contains(3)){println(b.apply(3)(0),b.apply(3)(1))}
      b -=(outD)
      a -=(outD)
      b.foreach(q =>{
        if(a.contains(q._1)){
          a.apply(q._1)(0) = math.max(a.apply(q._1)(0),q._2(0))
          //a.apply(q._1)(1) = math.max(a.apply(q._1)(1),q._2(1))
          a.apply(q._1)(1) += q._2(1)
        }else{
          a += q
        }
      }
        )
      a += (outD -> outDeg)
      println("=============")
      if(a.contains(3)){println(a.apply(3)(0),a.apply(3)(1))}
    return a
  }
    
  val iniMessage = new HashMap[VertexId, Array[Double]]()
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, messageCombiner)
  
  ppr.vertices.collect
  
  println("finished!")
  
}