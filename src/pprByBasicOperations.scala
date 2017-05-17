import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import collection.mutable.HashMap

/*
 * to computer personalized page rank by using those basic GraphX operations(no Pregel)
 */
class pprByBasicOperations {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  val edgesFile = "/local/liy/data/data_test.txt"

  //partitions number
  val numPartitions = 100
  
  //iterations number:
  //BFS depths for each iteration: 2, 4, 8, ...
  //note that the initial depth(pre-iterations) is 1
  val numIterations = 2

  //BFS depth, corresponding to numIterations
  //numIterations = log2(depthOfBFS)
  val depthOfBFS = 4
  
  //teleportation constant
  val ct = 0.15
  
  //a function to combine the vertex's hashmap with those of its neighbors
  def hmMerge(hostHM:HashMap[VertexId, Array[Double]], verts: VertexRDD[HashMap[VertexId, Array[Double]]]) : HashMap[VertexId, Array[Double]] = {
    //
    //val tempHM = new HashMap[VertexId, Array[Double]]() ++= hostHM
    val tempHM = new HashMap[VertexId, Array[Double]]()
    hostHM.foreach(t =>
      {
        val id:VertexId = t._1
        var arr = new Array[Double](t._2.length)
        val z = 0
        for(z <- 0 until t._2.length){
          arr(z) = t._2(z)
        }
       tempHM += (id -> arr)
        
      })
    hostHM.foreach(p =>
      {
        //to get the hashmap of this target-vertex
        val neighborHM = verts.partitionsRDD.flatMap { part =>
            if (part.isDefined(p._1)) Some(part(p._1))
            else None
            }.collect.head
            
        //val nHM = verts.filter{case(id,_) => id==p._1}.collect.head._2
        //max
        neighborHM.foreach(q => {
          val maxLen = p._2(0)+q._2(0)
          val tempLen = p._2(1)+q._2(1)
          //val tempScore = p._2(2)*q._2(2)
          val tempScore = tempHM.apply(p._1)(2)*q._2(2)
          if(hostHM.contains(q._1)){
            if(maxLen > p._2(0)){
              tempHM.apply(q._1)(2) = tempHM.apply(q._1)(2) + tempScore
              tempHM.apply(q._1)(0) = math.max(tempHM.apply(q._1)(0),maxLen)
              tempHM.apply(q._1)(1) = math.max(tempHM.apply(q._1)(1),tempLen)
            }
            
          }else{
            if(tempHM.contains(q._1)){
              //val maxLen = p._2(0)+q._2(0)
              //val tempLen = p._2(1)+q._2(1)
              //val tempScore = p._2(2)*q._2(2)
              tempHM.apply(q._1)(0) = math.max(tempHM.apply(q._1)(0),maxLen)
              tempHM.apply(q._1)(1) = math.max(tempHM.apply(q._1)(1),tempLen)
              tempHM.apply(q._1)(2) = tempHM.apply(q._1)(2) + tempScore
            }else{
              tempHM += (q._1 -> Array(p._2(0)+q._2(0), p._2(1)+q._2(1), tempScore))
            }
          }
        }
            )
      }
        )
    tempHM.foreach(t =>
      {
        t._2(0) = t._2(1)
      }
        )  
    
    return tempHM
  }
  
  // to construct the initial hashmap of each vertex before PPR computation
  // Array[VertexId] => HashMap[VertexId, Array[Double](3)] 
  // the 1st element of DoubleArray is the max length of paths from src_vertex to target_vertex in previous steps
  // the 2nd one is the temp max-length in current step, will be used to update 1st element when the entire step is finished.
  // the 3rd one is the scores(inverse P-distance)
  
  def initialHashMap(arr:Array[VertexId]):HashMap[VertexId, Array[Double]] = {
    val hm = new HashMap[VertexId, Array[Double]]()
    for(id <- arr){
      hm += (id -> Array(1,1,ct/arr.length))
    }
    return hm
  }
  
  
  // to access an vertex using its Vid, and which properties will be returned.
  // here, a hashmap is returned.
  /*
  def getProperty(Vid: Long, g: Graph[HashMap[VertexId, Array[Double]],Int]):Option[HashMap[VertexId, Array[Double]]] = {
   val verts: VertexRDD[_] = g.vertices
   //val targetVid: VertexId = 80L
   val result = verts.partitionsRDD.flatMap { part =>
     if (part.isDefined(Vid)) Some(part(Vid))
     else None
   }.collect.head
   
   return result
  }
  */
  
  
  //to construct the graph by loading an edge file on disk
  val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions).partitionBy(PartitionStrategy.EdgePartition2D)
  
  //to add a hashmap to each vertex(for now, it is the only vertex-property)
  //{(path_length : distance_score), ...}
  //val initialGraph: Graph[HashMap[VertexId, HashMap[Int, Long]], Int] = graph.mapVertices{ (id, _) => HashMap[VertexId, HashMap[Int, Long]](id ->HashMap[Int, Long](0 -> 10L))}
  val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}
  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  var initialGraph = Graph(initialVertexRDD, graph.edges).cache
  
  //the loop to compute PPR of this graph
  val i = 0
  for(i <- 0 until numIterations){
    var vRDD = initialGraph.vertices
    //val initialGraph: Graph[HashMap[VertexId, Array[Double]], Int] = initialGraph.mapVertices{case(id:VertexId, hm:HashMap[VertexId, Array[Double]]) => hmMerge(hm, vRDD)}
    initialGraph = initialGraph.mapVertices((id, attr) => hmMerge(attr, vRDD))
    //initialGraph = newGraph
  }
  initialGraph.vertices.collect
  
  def getNeighbors(vID:VertexId):HashMap[VertexId, Array[Double]] ={
    val neighborHM = initialGraph.vertices.partitionsRDD.flatMap { part =>
            if (part.isDefined(vID)) Some(part(vID))
            else None
            }.collect.head
    return neighborHM
  }
  
  
}