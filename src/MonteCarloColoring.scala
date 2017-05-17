import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import collection.mutable.HashMap
import scala.util.Random
import java.io._

/*
 * to computer personalized page rank from a set of hubs:
 * 1) pre-computing is finished in this program: finally every vertex will maintain a hubs-visited vector
 * - multiple walks(colorings) for one vertex in one superstep
 * - colorings are synchronic on graphx, v.s asynchronic on graphlab??
 */
class MonteCarloColoring {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  //edge file format: srtId -> tarId
  //val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
  //hub vertices file format: vId valueInPerferenceVector
  //val landmarksFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
  //val resultFilePathOnHDFS = "/Users/yifanli/Data/PPR"
    
  //input files on HDFS
  val edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-LiveJournal1.txt"
  //val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
  val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/FullLandmarks.txt"
  val resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_Full"
    

  //partitions number
  val numPartitions = 20
  
  //the walks for each vetex as below:
  //Notice: here only for test, it should be calculated from wlen by following geometric distribution Geom(ct)
  val walks = Array(2,3,5,6,9,15,20,27,35,43,50,60)
  
  //iterations number:
  //NOTICE! -- this number should be equal to the last walk length in walks
  val numIterations = walks(walks.length-1)
  
  //the total length of walks for each vertex
  val wlen = 170
  
  //teleportation constant
  val ct = 0.15
  val Diff_ct = 1-ct
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
    
  // to randomly pick only "ONE" vertex from its neighbors
  def randomNeighbor(neighbors:Array[VertexId]) : VertexId= {
    if(neighbors.length == 0){
    	return -1
    }else{
    	return neighbors(Random.nextInt(neighbors.length))
    }
  }
  
  /*
   * to construct the initial hashmap of each vertex before PPR computation
   * this hashmap[(VertexId,Int), (Int,VertexId)] is used to store those walks that will be distributed
   * [(VertexId, walk_id) -> (walk_length, targetNeigh_Vid)]
   */
  
  def initialHashMap(vid:VertexId, v: (Array[VertexId],Double)):HashMap[(VertexId,Int), (Int,VertexId)] = {
    val hm = new HashMap[(VertexId,Int), (Int,VertexId)]()
    if(v._2>0.0){
      //hm +=(vid -> v)
      for(i <- 0 to (walks.length - 1)){
        hm +=((vid,i+1) -> (walks(i),randomNeighbor(v._1)))
      }
    }
    
    return hm
  }
  
  // to initialize a vector for each vertex to count the visited-times of every hub
  def initialVector(vid:VertexId, v:(Array[VertexId],Double)):HashMap[VertexId, Int] = {
    val hm = new HashMap[VertexId, Int]()
    if(v._2>0.0){
      hm += (vid -> walks.length)
    }
    return hm
  }
  
  //to construct the graph by loading an edge file on disk
  val t1 = System.currentTimeMillis
  val graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, numEdgePartitions = numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK_SER,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK_SER).partitionBy(PartitionStrategy.EdgePartition2D)
  val t2 = System.currentTimeMillis
  // to load the hub vertices file to construct a hub vertices RDD[(VertexId, Double)]
  val hubRDD: RDD[(VertexId, Double)] = sc.textFile(landmarksFileOnHDFS).coalesce(numPartitions).map(_.split('\t')).map(line => (line(0).trim.toLong, line(1).trim.toDouble))
  
  //val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}.persist(StorageLevel.MEMORY_AND_DISK)
  
  // to get the out degrees of each vertex
  //val outDeg: RDD[(VertexId, Int)] = graph.outDegrees
  
  //val firstRondomedVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, (setInit(id,1,numOfSamplings), (a, iniWalksDistribution(a))))}.persist(StorageLevel.MEMORY_AND_DISK_SER)
  
  //to construct the vertices RDD by joining with hubs preference values
  val verRDD = graph.collectNeighborIds(EdgeDirection.Out).leftJoin(hubRDD)((id, a, b) => (a,b.getOrElse(0.0))).map{case(id:VertexId,v:(Array[VertexId],Double)) => (id, (v,(initialVector(id,v), initialHashMap(id,v))))}.persist(StorageLevel.MEMORY_AND_DISK_SER)

  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  //var initialGraph = Graph(initialVertexRDD, graph.edges).persist(StorageLevel.MEMORY_ONLY)
  //val edgeStorageLevel=StorageLevel.MEMORY_AND_DISK
  //var initialGraph = Graph(verRDD, graph.edges)
  var initialGraph = Graph(verRDD, graph.edges,null,edgeStorageLevel = MAD, vertexStorageLevel = MAD)
  
  /*
   * to construct those 3 functions for GraphX version of Pregel
   * 
   */
  def vertexProgram(id: VertexId, attr: ((Array[VertexId],Double),(HashMap[VertexId, Int],HashMap[(VertexId,Int), (Int,VertexId)])), msgSum: HashMap[(VertexId,Int), Int]):((Array[VertexId],Double),(HashMap[VertexId, Int],HashMap[(VertexId,Int), (Int,VertexId)]))={
    if((msgSum.size == 1) && msgSum.contains((0L,0))){
      //attr._2._2._2 ++= attr._2._2._1
      //attr._2._2._1.clear
      return attr
    }
    
    val dis = new HashMap[(VertexId,Int), (Int,VertexId)]()
    msgSum.foreach(p => {
      
      attr._2._1.update(p._1._1, 1 + attr._2._1.getOrElse(p._1._1, 0))
      if(p._2>1){
        dis += (p._1 -> ((p._2 -1),randomNeighbor(attr._1._1)))
      }
      
    }
      )
    //println("************")
    //println(id)
    //println(msgSum)
    //println("************")
    return (attr._1, (attr._2._1,dis))
  }
  
  //Notice: the target Vid might be -1
  def sendMessage(edge: EdgeTriplet[((Array[VertexId],Double),(HashMap[VertexId, Int],HashMap[(VertexId,Int), (Int,VertexId)])), Int]) = {
    val hm = new HashMap[(VertexId,Int), Int]()
    edge.srcAttr._2._2.foreach(p => {
      if(p._2._2 == edge.dstId){
        hm += ((p._1 -> p._2._1))
      }
    }
        )
    if(edge.srcAttr._2._2.isEmpty){
      Iterator.empty
    }else{
      Iterator((edge.dstId, hm))
    }
  }
  def messageCombiner(a:HashMap[(VertexId,Int), Int], b:HashMap[(VertexId,Int), Int]):HashMap[(VertexId,Int), Int] = {
    return a++b
  }
    
  //val iniMessage = HashMap.empty[(VertexId,Int), Int]
  val iniMessage = HashMap[(VertexId,Int), Int]((0L,0) -> 0)
  //val iniMessage = new HashMap[VertexId, Double]()
  val t3 = System.currentTimeMillis
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  val t4 = System.currentTimeMillis
  //ppr.vertices.collect
  //ppr.vertices.saveAsTextFile(resultFilePath)
  println("the time(ms) of graph loading: "+(t2 - t1))
  println("the time(ms) of pregel computation: "+(t4 - t3))
  
  val writer = new PrintWriter(new File("times.txt" ))
  writer.write("the time(ms) of graph loading: "+(t2 - t1).toString)
  writer.write("the time(ms) of pregel computation: "+(t4 - t3).toString)
  writer.close()
  
  ppr.vertices.saveAsTextFile(resultFilePathOnHDFS)
 
  
  println("finished!")
  
}