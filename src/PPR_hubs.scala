import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import collection.mutable.HashMap

/*
 * to computer personalized page rank from a set of hubs:
 * 1) pre-computing is finished in this program: the rank values of each hub is be transfered to its neighbors
 * Notice:
 * - only incremental values will be transfer to next superstep for calculation
 * - the vertex who has no in-messages will be halted
 */
class PPR_hubs {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  //edge file format: srtId -> tarId
  val edgesFile = "/Users/yifanli/Data/edges_test.txt"
  //hub vertices file format: vId valueInPerferenceVector
  val hubVertexFile = "/Users/yifanli/Data/onlyHubs_test.txt"
    
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
  val Diff_ct = 1-ct
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1
  
  //a function to combine the vertex's hashmap with those of its neighbors
  def hmCal(srcAtt: (Int,HashMap[VertexId,Double])) : HashMap[VertexId, Double] = {
    val hm = new HashMap[VertexId, Double]()
    srcAtt._2.foreach(p =>{
      //tempHM += (p._1 -> p._2.*(iniScore))
      println("++++++++++++++++++++")
      println(p._2)
      //srcAtt._2.update(p._1, p._2*(1/srcAtt._1.toDouble)*Diff_ct)
      hm += ((p._1 -> p._2*(1/srcAtt._1.toDouble)*Diff_ct))
      println("*******************")
      println(p._2*(1/srcAtt._1.toDouble)*Diff_ct)
      println("*******************")
      println(srcAtt._2.apply(p._1))
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
  val graph = GraphLoader.edgeListFile(sc, edgesFile, false, numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
  
  // to load the hub vertices file to construct a hub vertices RDD[(VertexId, Double)]
  val hubRDD: RDD[(VertexId, Double)] = sc.textFile(hubVertexFile).coalesce(numPartitions).map(_.split('\t')).map(line => (line(0).trim.toLong, line(1).trim.toDouble))
  
  //val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}.persist(StorageLevel.MEMORY_AND_DISK)
  
  // to get the out degrees of each vertex
  val outDeg: RDD[(VertexId, Int)] = graph.outDegrees
  
  //to construct the vertices RDD by joining with hubs preference values
  val verRDD = graph.vertices.leftJoin(hubRDD)((id, a, b) => a+b.getOrElse(0.0)-1.0).map{case(id:VertexId,v:Double) => (id, (initialHashMap(id,v), initialHashMap(id,v)))}.persist(StorageLevel.MEMORY_AND_DISK)
  //val verRDD = graph.vertices.leftJoin(hubRDD)((id, a, b) => a+b.getOrElse(0.0)-1.0).map{case(id:VertexId,v:Double) => (id, (initialHashMap(id,v), initialHashMap(id,v)))}
  
  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  //var initialGraph = Graph(initialVertexRDD, graph.edges).persist(StorageLevel.MEMORY_ONLY)
  //val edgeStorageLevel=StorageLevel.MEMORY_AND_DISK
  var initialGraph = Graph(verRDD, graph.edges).outerJoinVertices(outDeg){(vid, hmc, optDeg) => (optDeg.getOrElse(0), hmc)}
  
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
    if(msgSum.contains(-1L) && msgSum.apply(-1L).equals(0.0) && msgSum.size.equals(1)){
      return attr
    }
    
    println("aaaaa")
    msgSum.foreach(p => {
    	attr._2._1.update(p._1, p._2 + attr._2._1.getOrElse(p._1, 0.0)*ct)
    	//for accurate rank value:
    	//attr._2._1.update(p._1, p._2 + attr._2._1.getOrElse(p._1, 0.0)*ct)
    }
      )
    return (attr._1, (attr._2._1,msgSum))
  }
  
  def sendMessage(edge: EdgeTriplet[(Int,(HashMap[VertexId, Double],HashMap[VertexId, Double])), Int]) = {
    if(edge.srcAttr._2._2.isEmpty){
      Iterator.empty
    }else{
      Iterator((edge.dstId, hmCal(edge.srcAttr._1,edge.srcAttr._2._2)))
    }
  }
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
    
  val iniMessage = HashMap(-1L -> 0.0)
  //val iniMessage = new HashMap[VertexId, Double]()
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  
  //ppr.vertices.collect
  ppr.vertices.saveAsTextFile(resultFilePath)
  
  println("finished!")
  
}