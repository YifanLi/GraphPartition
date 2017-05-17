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
 * to compute Fully Personalized PageRank(FPPR) using the approximation approach Monte Carlo
 * Ref. "Fast Personalized PageRank on MapReduce"
 * 
 * 
 */
class FPPR_MonteCarlo {
  // Connect to the Spark cluster
  //val sc = new SparkContext("localhost", "PersonalizedPR")
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  val edgesFile = "/Users/yifanli/Data/data_test.txt"
  val resultFilePath = "/Users/yifanli/Data/PPR_MC"

  //partitions number
  val numPartitions = 2
  
  //iterations number:
  //BFS depths for each iteration: 2, 4, 8, ...
  //note that the initial depth(pre-iterations) is 1
  val numIterations = 1

  //the total PPR walk length L for each vertex
  val wL = 200
  
  //teleportation constant
  val ct = 0.15
  
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1
  
  //the Delta(incrementation) to decide if a message should be transfered
  val Delta = 0.001
  
  //the number of samplings
  val numOfSamplings = 10
  
  //the length of a segment
  val lenOfSeg = 5
  
  // to randomly pick "some" vertices from its neighbors
  def randomNeighbor(neighbors:Array[VertexId], numOfRam: Int) : Array[VertexId]= {
    if(neighbors.length == 0){
    	return null
    }else{
    	val arr = new Array[VertexId](numOfRam)
    	for(i <- 0 to arr.length-1){arr(i) = neighbors(Random.nextInt(neighbors.length))}
    	return arr
    	//return neighbors(Random.nextInt(neighbors.length))
    }
  }
  
  // to randomly pick only "ONE" vertex from its neighbors
  def randomNeighbor(neighbors:Array[VertexId]) : VertexId= {
    if(neighbors.length == 0){
    	return -1
    }else{
    	return neighbors(Random.nextInt(neighbors.length))
    }
  }
  
  // to initialize the set of segments from 1st randomly choose before pregel iteration computation
  def setInit(srcID:VertexId, tag: Int, num: Int):scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])] ={
    val setOfsegs = scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]()
    if(num<1){
      return null
    }else{
      if(tag==1){
        for(i <- 1 to num){
          val buf = scala.collection.mutable.ArrayBuffer.empty[VertexId]
          //buf += arr(i)
          setOfsegs += (((srcID,(i,1)),buf))
        }
        //return setOfsegs
      }
      
    }
    return setOfsegs
  }
  
  // to append the randomly chosen neighbor to that segment(an array buffer)
  def appendSeg(newEndNode:VertexId, seg: ((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])):scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])] ={
   seg._2 += newEndNode
   val setOfsegs = scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]()
   setOfsegs += seg
   return setOfsegs
  }
  
  
  //to construct the graph by loading an edge file on disk
  val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
  
  //to add a hashmap to each vertex(for now, it is the only vertex-property)
  //{(path_length : distance_score), ...}
  //val initialGraph: Graph[HashMap[VertexId, HashMap[Int, Long]], Int] = graph.mapVertices{ (id, _) => HashMap[VertexId, HashMap[Int, Long]](id ->HashMap[Int, Long](0 -> 10L))}
  
  //notice that here only one neighbor is picked for each sampling.
  val firstRondomedVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, (setInit(id,1,numOfSamplings), a))}.persist(StorageLevel.MEMORY_AND_DISK)
  
  var initialGraph = Graph(firstRondomedVertexRDD, graph.edges)
  
  /*
   * <-------Segments Building-------->
   * 1) w.r.t the algorithm proposed by Cedric
   * 2) notice that the samplings number and segment length are both pre-set.
   * 3) the vertex attribute(VD):
   *    (set of ((srcVId,(PathId,SegmentId)), segment to current vertex), array of out-going neighbors)
   */
  val iniMessage = scala.collection.mutable.Set.empty[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]
  
 /* 
  def vertexProgram(id: VertexId, attr: (scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])],Array[VertexId]), msgSum: scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]):(scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])],Array[VertexId])={
    if(attr._2.length == 0){
      return (attr._1++msgSum, attr._2)
    }else{
      if(msgSum.size == 0){
        return attr
      }else{
    	  return (msgSum, attr._2)
      }
    }
  }
 */
  def vertexProgram(id: VertexId, attr: (scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])],Array[VertexId]), msgSum: scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]):(scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])],Array[VertexId])={
    if(attr._2.length == 0){
      return (attr._1++msgSum, attr._2)
    }else{
      if(msgSum.size == 0){
        return attr
      }else{
    	  return (msgSum, attr._2)
      }
    }
  }

  /*
  def sendMessage(edge: EdgeTriplet[(scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])],Array[VertexId]), Int]): Iterator[(VertexId, scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])])]={
	val buf = scala.collection.mutable.ArrayBuffer.empty[(VertexId, scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])])]
    //println(edge.srcAttr._1.size)
    //println(edge.srcAttr._1)
    //println("***********************")
	//
	println(edge.srcId)
	edge.srcAttr._1.foreach(f => println(f))
	//println("***********************")
    edge.srcAttr._1.foreach(x => {val node = randomNeighbor(edge.srcAttr._2) 
    if(node != -1){buf += ((node,appendSeg(node,x)))} })
    
    if(buf.isEmpty){
      return Iterator.empty
    }else{
      //println(buf)
      //println("$$$$$$$$$$$$$$$$$$$$$$$")
      return buf.iterator
    }
  }
  */
  def sendMessage(edge: EdgeTriplet[(scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])],Array[VertexId]), Int]): Iterator[(VertexId, scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])])]={
	//val buf = scala.collection.mutable.ArrayBuffer.empty[(VertexId, scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])])]
    //println(edge.srcAttr._1.size)
    //println(edge.srcAttr._1)
    println("***********************")
	//
	val mesgHM = new HashMap[VertexId, Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]()
	println(edge.srcId)
	edge.srcAttr._1.foreach(f => println(f))
	//println("***********************")
    edge.srcAttr._1.foreach(x => {val node = randomNeighbor(edge.srcAttr._2) 
    if(node != -1){
      if(mesgHM.contains(node)){
        mesgHM += (node -> (mesgHM.apply(node)++appendSeg(node,x)))
      }else{
        mesgHM += (node -> appendSeg(node,x))
      }
      
      }})
    
    if(mesgHM.isEmpty){
      return Iterator.empty
    }else{
      println(mesgHM)
      println("$$$$$$$$$$$$$$$$$$$$$$$")
      return mesgHM.iterator
    }
  }
  
  def messageCombiner(a:scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])], b:scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]):scala.collection.mutable.Set[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])] = {
	//val c = new HashMap[VertexId, Double]()	
    return a ++ b
  }
    
  //val iniMessage = scala.collection.mutable.Set.empty[((VertexId, (Int,Int)), scala.collection.mutable.ArrayBuffer[VertexId])]
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  
  //to output ppr.vertices.collect
  ppr.vertices.saveAsTextFile("/Users/yifanli/Data/PPR_temp")
  //to create a new vertices RDD (srcVId, segToIt)
  val nullSeg = (-100L,Set.empty[((Int,Int),ArrayBuffer[VertexId])])
  val segs: RDD[(VertexId, Set[((Int,Int),ArrayBuffer[VertexId])])] = ppr.vertices.flatMap(e => e._2._1.map(i => {if(!i._2.isEmpty) (i._1._1,Set((i._1._2,i._2))) else nullSeg} )).persist(StorageLevel.MEMORY_AND_DISK)
  //note that pre-set numPartitions might improve the performance in reduceByKey operation
  //note that "avoiding groupByKey" as much as possible
  val segsNodup: RDD[(VertexId, Set[((Int,Int),ArrayBuffer[VertexId])])] = segs.reduceByKey((a,b) => a++b)
  
  //combine the result above with graph
  val newVertices = ppr.vertices.leftJoin(segsNodup)((vid, a, b) => b)
  //to construct the new graph(which keeps all segments)
  val segsGraph = Graph(newVertices,ppr.edges)
  //to output vertices.collect
  segsGraph.vertices.saveAsTextFile(resultFilePath)
  //to output edges.collect
  //segsGraph.edges.saveAsTextFile(resultFilePath)
  

}