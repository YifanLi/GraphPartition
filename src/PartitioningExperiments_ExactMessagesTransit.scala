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
 * to compute **Fully** Random Walks using the approximation approach Monte Carlo
 * Ref. "Fast Personalized PageRank on MapReduce"
 * 
 * Usage:
 * => ./bin/spark-submit --class FPPRMC ~/FPPRMC_v1.jar [partitions] [spark.executor.memory] [max.cores] [numIterations] [partitionStrategy] [defaultOrBlocked] [numOfSamplings] [PartitonsToLoad:201;298;300;Pokec200refined]
 * e.g.
 * nohup /usr/local/spark1/spark-1.5.2-bin-hadoop1/bin/spark-submit --driver-memory 30G --class "PartitioningExperiments_ExactMessagesTransit" ~/graphx-emt_2.10-0.1.jar 205 40G 250 4 3 defaultPartitioning 2 Pokec_MerBlo200 &
 */

object PartitioningExperiments_ExactMessagesTransit {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ExactMessagesTransit_Yifan")
    conf.set("spark.ui.port","4000")
    conf.set("spark.master","spark://small1-tap1:7077")
    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.task.maxFailures", "128")
    conf.set("spark.speculation", "false")
    conf.set("spark.shuffle.manager", "SORT")
    conf.set("spark.shuffle.consolidateFiles","true")
    // Compress RDD in both memory and on-disk using the fast Snappy compression codec
    //conf.set("spark.rdd.compress", "true")
    //The codec used to compress internal data such as RDD partitions, broadcast variables and shuffle outputs. 
    //By default(snappy), Spark provides three codecs: lz4, lzf, and snappy.
    conf.set("spark.io.compression.codec","lz4")

    conf.set("spark.cores.max",args(2))
    //Number of cores to allocate for each task. 1 by default
    conf.set("spark.task.cpus","3")
    conf.set("spark.executor.memory",args(1))
    //Limit of total size of serialized results of all partitions for each Spark action (e.g. collect)
    conf.set("spark.driver.maxResultSize","5g")
    
    //fraction of spark.executor.memory which will be used for in-memory storage; to decrease it if need more memory for computation(e.g. JVM)
    conf.set("spark.storage.memoryFraction", "0.3")
    //fraction of spark.executor.memory which will be used for shuffle
    conf.set("spark.shuffle.memoryFraction", "0.4")
    //If set to "true", consolidates intermediate files created during a shuffle
    conf.set("spark.shuffle.consolidateFiles","true")
    
    conf.set("spark.eventLog.enabled", "true")
    // Help prevent FetchFailed exceptions when single node is heavily taxed
    // http://stackoverflow.com/questions/26247654/spark-fail-to-run-the-terasort-when-the-amount-of-data-gets-bigger
    conf.set("spark.core.connection.ack.wait.timeout", "6000")
    // Attempting to fix "Map output statuses were 10810591 bytes which exceeds spark.akka.frameSize (10485760 bytes)"
    conf.set("spark.akka.frameSize", "1000")
    conf.set("spark.akka.timeout","6000")
    //Size of each piece of a block in kilobytes for TorrentBroadcastFactory.
    conf.set("spark.broadcast.blockSize","10240")
    conf.set("spark.akka.threads","12")
    
    //to set the minimal parallelism to ensure only one partition for each small input file
    conf.set("spark.default.parallelism","1")

    val sc = new SparkContext(conf)
    
    var numParName = args(7)
    
    //input files on HDFS
    var edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-PokecRelationships.txt"
    if(numParName.startsWith("Pokec")){
    	edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-PokecRelationships.txt"
    }else{
    	edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-LiveJournal1.txt"
    }
    //val edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //val landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
    val seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top200deg.txt"
    //val seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    val resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_blockpartition"
    //the directory to store the files of edge partitions
    //val parFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/edgePartitions"
    
    //the directory to store the 201 files of edge partitions
    //val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/201_6_partitions"
    //the refined partitions from 201_6_partitions:264
    val edgeParFilesPathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/"+numParName
     

    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number:
  val numIterations = args(3).toInt
  
  //a tag to indicate the input dataset: 
  // 1, soc-LiveJournal1.txt for 2D partitioning
  // 2, 201_6_partitions for blocked partitioning
  //val TAG = args(4).toInt

  //the total PPR walk length L for each vertex
  val wL = 200
  
  //teleportation constant
  val ct = 0.15
  
  
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1
  
  //the number of samplings
  val numOfSamplings = args(6).toInt
  
  //the length of a segment
  val lenOfSeg = 5

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
  
  //the partitioning strategy
  //4 options in graphx: CanonicalRandomVertexCut, EdgePartition1D, EdgePartition2D, RandomVertexCut
  //val partitionStrategy = PartitionStrategy.EdgePartition2D
  val strategies = Array(PartitionStrategy.CanonicalRandomVertexCut,PartitionStrategy.EdgePartition1D, PartitionStrategy.EdgePartition2D, PartitionStrategy.RandomVertexCut)
  val numOfStra = args(4).toInt
  val partitionStrategy = strategies(numOfStra)
  
  //to indicate which dataset will be loaded
  val ind = args(5)
  
  //to construct the graph by loading an edge file on disk
  //NOTICE: for Spark 1.1, the number of partitions is set by minEdgePartitions
  var t1=0L
  var t2=0L
  var graph:Graph[Int,Int] = null
  
  if(ind.equals("defaultPartitioning")){
  println("+++++++> partition strategy:"+partitionStrategy.toString()+"<+++++++++")
  t1 = System.currentTimeMillis
  //graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions, edgeStorageLevel = MAD,vertexStorageLevel = MAD).partitionBy(partitionStrategy)
  graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions).partitionBy(partitionStrategy)
  t2 = System.currentTimeMillis
  }else{
  /*
   * to build graph using our blocks  
   */ 
  println("-------> blocked partitioning <-------")
  t1 = System.currentTimeMillis
  //graph = GraphLoader.edgeListFile(sc, edgeParFilesPathOnHDFS, false, -1, edgeStorageLevel = MAD,vertexStorageLevel = MAD)
  graph = GraphLoader.edgeListFile(sc, edgeParFilesPathOnHDFS, false, -1)
  t2 = System.currentTimeMillis
  }
  
  println("##############################")
  println("num of edge partitions: "+graph.edges.partitions.size.toString)
  println("##############################")
     
  //notice that here only one neighbor is picked for each sampling.
  //val outNeighbors = graph.collectNeighborIds(EdgeDirection.Out)
  val outNeighbors = graph.collectNeighborIds(EdgeDirection.Out).collectAsMap

  // to randomly pick only "ONE" vertex from its neighbors
  def randomNeighbor(neighbors:Array[VertexId]) : VertexId= {
    if(neighbors.length == 0){
    	return -1
    }else{
    	return neighbors(Random.nextInt(neighbors.length))
    }
  }
  
  // to initialize the set of segments from 1st randomly choose before pregel iteration computation
  def setInit(srcID:VertexId, tag: Int, num: Int):HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]] ={
    //println("#######################")
    //val neigs = outNeighbors.filter{case(id, _) => id==srcID}.first._2
    val neigs = outNeighbors.getOrElse(srcID, Array.empty[VertexId])
    //val neigs = Array(1L,2L,3L,4L,5L)
    //println("***************neigs_length: "+neigs.length)
    var disHM = HashMap.empty[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]
    if(num<1){
      return null
    }else{
      if(tag==1){
        for(i <- 1 to num){
          val setOfsegs = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
          val buf = ArrayBuffer.empty[VertexId]
          setOfsegs += (((srcID,(i,1)),buf))
          val tarId = randomNeighbor(neigs)
          //println("@@@@@@@@@@@@@@tarId:"+tarId.toString)
          //val tarId = 1
          disHM.update(tarId, setOfsegs ++ disHM.getOrElse(tarId, Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]))
          
          
        }
        //return setOfsegs
      }
      
    }
    return disHM
  }
  
  // to append the randomly chosen neighbor to those segments(an array buffer)
  def appendSeg(newEndNode:VertexId, segs: Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]):Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])] ={
   val newSegs = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
   for(seg <- segs){
     val buf = ArrayBuffer.empty[VertexId]
     buf ++= seg._2
     buf += newEndNode
     newSegs += ((seg._1, buf))
     }
   return newSegs
  }
  // to construct a hashmap that keeps the messages will be distributed to (out-going) neighbors
  def iniWalksDistribution(neighbors:Array[VertexId]):HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]] ={
    var disHM = HashMap.empty[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]
    if(neighbors.isEmpty) return disHM
    for(e <- neighbors){
      disHM += (e -> Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])])
    }
    return disHM
  }
  
  val firstRondomedVertexRDD = graph.vertices.map{case(id:VertexId, a:Int) => (id, setInit(id,1,numOfSamplings))}
  // val firstRondomedVertexRDD = ~.persist(MAD).coalesce(numPartitions,true)
  
  //var initialGraph = Graph(firstRondomedVertexRDD, graph.edges)
  var initialGraph = Graph(firstRondomedVertexRDD, graph.edges,null,edgeStorageLevel = MAD, vertexStorageLevel = MAD)
  
  /*
   * <-------Segments Building-------->
   * 1) w.r.t the algorithm proposed by Cedric
   * 2) notice that the samplings number and segment length are both pre-set.
   * 3) the vertex attribute(VD):
   *    (hashmap [targetVertexId, set of ((srcVId,(PathId,SegmentId)), path to current vertex)])
   */
  val iniMessage = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
  iniMessage += (((-10L,(0,0)),ArrayBuffer.empty[VertexId]))
  
  def vertexProgram(id: VertexId, attr: HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]], msgSum: Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]):HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]={
    //val neigs = outNeighbors.filter{_._1==id}.first._2
    val neigs = outNeighbors.getOrElse(id, Array.empty[VertexId])
    if(msgSum.size==1 && msgSum.head._1._1 == -10L){
        attr.foreach(x => {
        	//val node = randomNeighbor(neigs)
        	attr += (x._1 -> (appendSeg(x._1,x._2)))
       	}
       	)
       	return(attr)
       }else{
         var disHM = HashMap.empty[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]
         msgSum.foreach(y => {
           val nd = randomNeighbor(neigs)
           //val nd =1
           val tbuf = ArrayBuffer.empty[VertexId]
           tbuf ++= y._2
           tbuf += nd
           val ty = (y._1,tbuf)
           val setOfsegs = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
           setOfsegs += (ty)
           disHM.update(nd, setOfsegs ++ disHM.getOrElse(nd, Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]))
           
         }
         )
          
         return (disHM)
      
       }
     
  }

  def sendMessage(edge: EdgeTriplet[HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]], Int]): Iterator[(VertexId, Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])])]={
    val dis = edge.srcAttr.getOrElse(edge.dstId, Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])])
    if(dis.isEmpty){
      Iterator.empty
    }else{
      Iterator((edge.dstId,dis))
    }
  }
  
  def messageCombiner(a:Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])], b:Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]):Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])] = {
	//val c = new HashMap[VertexId, Double]()	
    return a ++ b
  }
  
  val t3 = System.currentTimeMillis
  //val iniMessage = Set.empty[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]
  val ppr = initialGraph.pregel(initialMsg = iniMessage, maxIterations=numIterations, activeDirection = EdgeDirection.Out)(vertexProgram, sendMessage, messageCombiner)
  val t4 = System.currentTimeMillis
  println("the time of graph computation(Pregel): "+(t4-t3))
  println("the time(ms) of graph loading: "+(t2 - t1))
  
  println("---VRF----")
  val numReplicatedVertices = initialGraph.edges.mapPartitions(part => Iterator(
  part.flatMap(e => Iterator(e.srcId, e.dstId)).toSet.size)).sum
  
  val numVertices = initialGraph.vertices.count
  println("the vertex replication factor: "+numReplicatedVertices/numVertices)
  println("----------")
  
  
  //to get the src vertex ids of walks that just arrived current vertex.
  def getSrcVidsOfWalks(vAttr: HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]):Array[VertexId] = {
    val buf = ArrayBuffer.empty[VertexId]
    vAttr.foreach(x =>{
      x._2.foreach(t => {
        buf += t._1._1
      }
        )
    }
      )
      
    return buf.toArray
  }
  
  //to combine multiple arrays into one
  def toCombineArrays(arrs: Array[Array[VertexId]]):Array[VertexId] = {
    val ARR = ArrayBuffer.empty[VertexId]
    arrs.foreach(a => {
      ARR ++= a
    }
      )
    
    return ARR.toArray
  }
  
  //to get all the passed vertices from paths arrived current vertex,
  //and to count them
  def getAllPassedVidsOfWalks(vAttr: HashMap[VertexId,Set[((VertexId, (Int,Int)), ArrayBuffer[VertexId])]]):HashMap[VertexId, Int] = {
    val buf = ArrayBuffer.empty[VertexId]
    vAttr.foreach(x =>{
      x._2.foreach(t => {
        //buf += t._1._1
        buf ++= t._2
      }
        )
    }
      )
      
    val c = new HashMap[VertexId, Int]()
    for(z <- buf){
      c.update(z, 1+c.getOrElse(z, 0))
    }
    return c
  }
  
  //a new vertices RDD containing all the passed vertices on each vertex
  var psVerRdd = initialGraph.vertices.mapValues(v => getAllPassedVidsOfWalks(v)).persist(MAD)
  //to extract all the passed vertices
  var extVerRdd = psVerRdd.flatMap(x => {x._2.map(e => (e._1, e._2))}).persist(MAD)
  //to count the accessed times of each vertex
  var timesRdd = extVerRdd.reduceByKey(_ + _).persist(MAD)
  
  //the vertices in each edge partition: a set[VertexId]
  val verInEdgePartitions = ppr.edges.mapPartitions(part => Iterator(part.flatMap(e => Iterator(e.srcId, e.dstId)).toSet)).toArray
  //to construct a hashmap to store the VRF(vertex replic factor) of each vertex
  val vrfHM = new HashMap[VertexId, Int]()
  for(x <- verInEdgePartitions(0)){
    vrfHM.update(x, 1)
  }
  for(i <- 1 to (verInEdgePartitions.length - 1)){
    for(x <- verInEdgePartitions(i)){
      vrfHM.update(x, 1+vrfHM.getOrElse(x, 0))
    }
  }
  
  //to make an RDD to store above hashmap
  var vrfRdd = sc.parallelize(vrfHM.toBuffer, numPartitions)
  
  //to count the total number of messages
  var totRdd = timesRdd.join(vrfRdd).mapValues(x => x._1*x._2)
  val accum = sc.accumulator(0L, "Accumulator_TotalNumOfMegs")
  totRdd.foreach(x => accum += x._2)
  println("=========the total number of messages during random walks===========")
  println(accum.value)
  println("====================")
  //
  
  
  
/*
 * ERROR: Total size of serialized results of 173 tasks (5.0 GB) is bigger than spark.driver.maxResultSize (5.0 GB)
 * 
  //the src vertex ids of walks who arrive this VERTEX-partition
  //val srcIdsOnPartition = ppr.vertices.mapPartitions(part => Iterator(toCombineArrays(part.flatMap(v => Iterator(getSrcVidsOfWalks(v._2))).toArray))).toArray
  
  //the src vertex ids of walks who arrive this EDGE-partition
  val srcIdsOnPartition = ppr.triplets.mapPartitions(part => Iterator(toCombineArrays(part.flatMap(etr => Iterator(getSrcVidsOfWalks(etr.srcAttr), getSrcVidsOfWalks(etr.dstAttr))).toArray))).toArray
  
  //the vertices in each (vertex? edge?)partition: a set[VertexId]
  val verInPartitions = ppr.edges.mapPartitions(part => Iterator(part.flatMap(e => Iterator(e.srcId, e.dstId)).toSet)).toArray
  //val verInPartitions = ppr.vertices.mapPartitions(part => Iterator(part.flatMap(v => Iterator(v._1)).toSet)).toArray
  
  //to count how many walks are from outside this partition
  var numOfCrossBoundaries = 0
  //for(i <- 0 to (ppr.vertices.partitions.size -1)){
  for(i <- 0 to (ppr.edges.partitions.size -1)){
    val verOfCurrentPar = verInPartitions(i)
    val srcIdsOfCurrentPar = srcIdsOnPartition(i)
    for(srcId <- srcIdsOfCurrentPar){
      if(!verOfCurrentPar(srcId)){
        numOfCrossBoundaries += 1
      }
    }
  }
  
  println("========================")
  println("the number of walks cross boundaries is: "+numOfCrossBoundaries.toString)
  
 */ 
  

  
  //to output ppr.vertices.collect
  //ppr.vertices.saveAsTextFile("/Users/yifanli/Data/PPR_temp")
  //ppr.vertices.saveAsTextFile(resultFilePathOnHDFS)
  //ppr.vertices.sample(false,0.1).saveAsTextFile(resultFilePathOnHDFS)
  println("Job done!")
    
  sc.stop()
    
  }

}