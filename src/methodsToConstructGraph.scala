import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import collection.mutable.HashMap
import java.io._
//import org.apache.spark.graphx.VertexId

/*
 * NOTICE:
 * 
 * MUST set the spark.default.parallelism to 1 to ensure the partition(for each small file) will not be split.
 */


class methodsToConstructGraph {
  
  val conf = new SparkConf().setAppName("xxxx")
  val sc = new SparkContext(conf)
  
  //file path
  val path = "/Users/yifanli/Data/data_test.txt"
  
  /*
   * 1) Load one edge per line in whitespace-delimited format: srcVertexId edgeAttr dstVertexId
   */
  val edges = sc.textFile(path).flatMap { line =>
    if (!line.isEmpty && line(0) != '#') {
      val lineArray = line.split("\\s+")
      if (lineArray.length < 2) {
        None
      } else {
        val srcId = lineArray(0).toLong
        //val attr = // parse lineArray(1) as appropriate
        val attr = lineArray(1)
        val dstId = lineArray(2).toLong
        Some(Edge(srcId, dstId, attr))
      }
    } else {
      None
    }
  }
 
  Graph.fromEdges(edges, 1)
  
  /*
   * 2) similar to above but vertex id is string
   */
  val triplets = sc.textFile(path).flatMap { line =>
  if (!line.isEmpty && line(0) != '#') {
    val lineArray = line.split("\\s+")
    if (lineArray.length < 2) {
      None
    } else {
      val t = new EdgeTriplet[String, String]
      t.srcId = lineArray(0).hashCode
      t.srcAttr = lineArray(0)
      t.attr = lineArray(1)
      t.dstId = lineArray(2).hashCode
      t.dstAttr = lineArray(2)
      Some(t)
    }
  } else {
    None
  }
}
 
val vertices = triplets.flatMap(t => Array((t.srcId, t.srcAttr), (t.dstId, t.dstAttr)))
val edges2 = triplets.map(t => t: Edge[String])
 
Graph(vertices, edges2)

  /*
   * 3) Load one edge per line in whitespace-delimited format: srcVertexId dstVertexId
   * by using Grpah.fromEdgeTuples()
   */
  val edgesTuples = sc.textFile(path).flatMap { line =>
    if (!line.isEmpty && line(0) != '#') {
      val lineArray = line.split("\\s+")
      if (lineArray.length < 2) {
        None
      } else {
        val srcId = lineArray(0).toLong
        //val attr = // parse lineArray(1) as appropriate
        //val attr = lineArray(1)
        val dstId = lineArray(1).toLong
        Some((srcId, dstId))
      }
    } else {
      None
    }
  }
 
  Graph.fromEdgeTuples(edgesTuples, 1)

val graph2 = GraphLoader.edgeListFile(sc, path, false, -1, edgeStorageLevel=StorageLevel.MEMORY_AND_DISK, vertexStorageLevel=StorageLevel.MEMORY_AND_DISK)
graph2.edges.mapPartitions(part => Iterator(part.flatMap(e => Iterator((e.srcId, e.dstId))).toSet)).collect
graph2.vertices.mapPartitions(part => Iterator(part.flatMap(v => Iterator(v)).toSet)).collect
  

}