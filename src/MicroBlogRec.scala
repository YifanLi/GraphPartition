import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import collection.mutable.HashMap

//import edu.sussex.nlp.*;
import edu.sussex.nlp.jws._

/*
 * to computer microblog recommendation scores
 */
class MicroBlogRec {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("MicroBlogRec")
  val sc = new SparkContext(conf)

  //file paths:
  //vertex file format: vId 
  //edge file format: srcID tarID topics
  //those 3 parts are separated by '\t', topics are separated by ','
  //val vertexFile = "/Users/yifanli/Data/microblogVertex_test.txt"
  val edgesFile = "/Users/yifanli/Data/microblogEdges_test.txt"
  //WARN: the output directory should not exist before, it will be created automatically
  val resultFilePath = "/Users/yifanli/Data/output"

  //partitions number
  val numPartitions = 8
  
  //iterations number:
  //BFS depths for each iteration: 1, 2, 3, ...
  //note that the initial depth(pre-iterations) is 1
  val numIterations = 1

  //BFS depth, corresponding to numIterations
  //numIterations = log2(depthOfBFS)
  val depthOfBFS = 4
  
  //decay factor of Katz
  val ct = 0.5
  val KF = 0.5
  
  //decay factor of Edge Relevence
  val ER = 0.5
  
  //to statically set those max-followed numbers of each topic are same value:
  //(NOTICE: those 20 values(suppose we have 20 topics) should be computed in advance)
  val maxFollowed = 100
  
  // the specific topic for recommendation
  val Topic = "t"
    
  // the base of Logarithm
  val BASE = 10
    
    
  /*
   * a function to compute the numbers of every following topic of that vertex
   * especially, a pair will be contained to keep the max number, whose key is "MAXNUM"
   */
  def toComputeNumOfTopics(inEdges:Array[Edge[Array[String]]]): HashMap[String, Int] = {
    val tempTopicNum = new HashMap[String, Int]()
    inEdges.foreach(edge =>{
      edge.attr.foreach(topic => {
        //val num = tempTopicNum.getOrElseUpdate(topic, 1)
        var newValue = 1
        if(tempTopicNum.contains(topic))
          newValue = tempTopicNum.apply(topic)+1          
        tempTopicNum.update(topic, newValue)
      })
    })
    //if(!tempTopicNum.contains("MAXNUM"))
    tempTopicNum.update("MAXNUM", tempTopicNum.maxBy(_._2)._2)
  
    return tempTopicNum
  }
  
  /*
   * the log function, derived from scala math library
   */
  def logB(x: Double, base: Int) = scala.math.log(x)/scala.math.log(base)
  
  /*
   * a function to compute the node authority for each topic within it
   * Notice: the log(), here, is being treated as ln()
   */
  def toComputeAuthorities(numOfTopics:HashMap[String, Int]):HashMap[String, Double] ={
    val auths = new HashMap[String, Double]()
    val maxNum = numOfTopics.apply("MAXNUM").toDouble
    numOfTopics.remove("MAXNUM")
    numOfTopics.foreach(tn => {
    	var localAuth = tn._2/maxNum
    	var globleAuth = logB(1+tn._2,BASE)/logB(1+maxFollowed, BASE)
    	
    	auths.update(tn._1,localAuth*globleAuth)
    })
    
    return auths
  }
  
  /*
   * a function to compute the max similarity between a edge and a specific topic
   * Notice: there may should be a static data structure to keep those similarities of each pair of topics,
   * witch can facilitate the computation of this function
   */
  def getMaxSim(topics: Array[String], top: String):Double = {
    var sim = 0.5
    //
    //
    //
    //
    return sim
  }
  
  // to load the edges file to construct a RDD[String]
  val microBlog: RDD[String] = sc.textFile(edgesFile).coalesce(numPartitions)
  
  // Parse the edges RDD to construct edgeRDD
  val microEdges: RDD[Edge[Array[String]]] = microBlog.map(_.split('\t')).
  // store the results in an object for easier access
  map(line => Edge(line(0).trim.toLong, line(1).trim.toLong, line(2).trim.split(',')))
  
  //to construct the graph by loading an edge file on disk
  //val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions).partitionBy(PartitionStrategy.EdgePartition2D)
  //So far, the default attribute for each vertex is 1.0L
  val graph = Graph.fromEdges(microEdges,1.0, edgeStorageLevel=StorageLevel.MEMORY_AND_DISK, vertexStorageLevel=StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
 
  //to get all in-edges for each vertex
  val inEdgesOfAll: VertexRDD[Array[Edge[Array[String]]]] = graph.collectEdges(EdgeDirection.In).persist(StorageLevel.MEMORY_AND_DISK)
  
  
  /*
   * Notice: the following calculation is based on that specific topic "t"
   */
  
  //class VertexProperty()
  case class NodeProperty(val authOnT: Double, val dstHM:HashMap[VertexId, Double])
  
  //var test = new HashMap[VertexId, Double]()
  //to computer the authority of specific topic "t" of each vertex
  val newVertices = inEdgesOfAll.mapValues(attr => new NodeProperty(toComputeAuthorities(toComputeNumOfTopics(attr)).getOrElse(Topic,0.0),new HashMap[VertexId, Double]())).persist(StorageLevel.MEMORY_AND_DISK)
  val tempVertices = graph.vertices.mapValues(v => new NodeProperty(0.0, new HashMap[VertexId, Double]())).persist(StorageLevel.MEMORY_AND_DISK)
  // to re-add those no-inEdge vertices into vertexRDD
  val vUnion = newVertices.union(tempVertices).persist(StorageLevel.MEMORY_AND_DISK)
  //to re-construct the new graph
  val newGraph = Graph(vUnion, graph.edges)
      

  /*
   * a function to combine the vertex's hashmap with those of its neighbors
   */
  def hmMerge(dstID:VertexId, dstPro:NodeProperty, edgeAttr: Array[String]) : HashMap[VertexId, Double] = {
    //println("ccccc")
    val tempHM = new HashMap[VertexId, Double]()
    val iniScore = KF*ER*dstPro.authOnT*getMaxSim(edgeAttr,Topic)
    dstPro.dstHM.foreach(p =>{
      tempHM += (p._1 -> (p._2*KF*ER + iniScore))
    }
      )
    //here, cycle is not considered, otherwise
    //dstHM += (dstID -> Array(lenToDst,scoreToDst*dstHM.apply(dstID)(1)))
    tempHM += (dstID -> iniScore)
    println("ddddd")
   
    return tempHM
  }
  
  
  
  /*
   * to construct those 3 functions for GraphX version of Pregel
   * vertexProgram(id: VertexId, attr: HashMap[VertexId, Array[Double]], msgSum: HashMap[VertexId, Array[Double]]):HashMap[VertexId, Array[Double]]
   * sendMessage(edge: EdgeTriplet[HashMap[VertexId, Array[Double]], HashMap[VertexId, Array[Double]]])
   * messageCombiner(a:HashMap[VertexId, Array[Double]], b:HashMap[VertexId, Array[Double]])
   * 
   */
  def vertexProgram(id: VertexId, attr: NodeProperty, msgSum: HashMap[VertexId, Double]):NodeProperty={
      println("aaaaa")
      val np = new NodeProperty(attr.authOnT, msgSum)
      return np
  }
  
  //messages distribution
  def sendMessage(edge: EdgeTriplet[NodeProperty, Array[String]]) =
    Iterator((edge.srcId, hmMerge(edge.dstId,edge.dstAttr,edge.attr)))
    
  //to define a "sum" function for processing all the messages received in a superstep 
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
  val tws = newGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.In)(vertexProgram, sendMessage, messageCombiner)
  
  //tws.vertices.collect
  tws.vertices.saveAsTextFile(resultFilePath)
  
  println("Twitter recommendation scores computation is finished!")
  
}