import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import collection.mutable.HashMap

class PageRank {
  val conf = new SparkConf().setAppName("PersonalizedPR")
  val sc = new SparkContext(conf)

  //file paths
  val edgesFile = "/Users/yifanli/Data/data_test.txt"
  val resultFilePath = "/Users/yifanli/Data/PPR_MC"

  //partitions number
  val numPartitions = 2
  
  //iterations number:
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
  
  val graph = GraphLoader.edgeListFile(sc, edgesFile, minEdgePartitions = numPartitions, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,
     vertexStorageLevel = StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.EdgePartition2D)
  
  val pagerankGraph: Graph[Double, Double] = graph.outerJoinVertices(graph.outDegrees) {
    (vid, vdata, deg) => deg.getOrElse(0)
  }.mapTriplets(e => 1.0 / e.srcAttr).mapVertices((id, attr) => 1.0)

def vertexProgram(id: VertexId, attr: Double, msgSum: Double): Double =
  ct + (1.0 - ct) * msgSum
def sendMessage(edge: EdgeTriplet[Double, Double]): Iterator[(VertexId, Double)] =
  Iterator((edge.dstId, edge.srcAttr * edge.attr))
def messageCombiner(a: Double, b: Double): Double = a + b
val initialMessage = 0.0
// Execute Pregel for a fixed number of iterations.
val pr = pagerankGraph.pregel(initialMessage, numIterations, EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)

}