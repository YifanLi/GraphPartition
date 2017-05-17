import org.apache.spark._
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.TreeSet
import java.io._
//import scala.util.control._


//import scala.util.Random
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.Set

/*
 * to partition the graph
 * 1) vertex-cut(edges partition)
 * 2) each seed vertice(e.g. Top300outdeg, Top1000outdeg) each block
 * 3) the value of seed will be propagated evenly to its neighbors for numIterations iterations
 * 4) TO merge blocks into partitions using 4/3 theorem by R. L. Graham
 * 
 * Usage:
 * => ./bin/spark-submit --class BlockedPartition ~/BlockPartition_v1.jar [partitions] [spark.executor.memory] [spark.cores.max] [iterations] [seeds file] [Pokec] [topk] [numOfPartitionsMerged]
 * e.g.
 * nohup /usr/local/spark1/spark-1.5.2-bin-hadoop1/bin/spark-submit --driver-memory 20G --class "BlockedPartition_MergeBlocks" ~/graphx-montecarlo-blocksmerge_2.10-0.1.jar 400 45G 300 4 2000 Pokec 5 200 &
 * 
 */

object BlockedPartition_MergeBlocks {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BlockPartition-BlocksMerge_Yifan")
    conf.set("spark.ui.port","4000")
    conf.set("spark.master","spark://small1-tap1:7077")
    
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //to set the maximum allowable size of Kryo serialization buffer; This must be larger than any object you attempt to serialize
    conf.set("spark.kryoserializer.buffer.max.mb", "1024")
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
    
    //fraction of spark.executor.memory which will be used for in-memory storage; to decrease it if need more memory for computation(e.g. JVM)
    conf.set("spark.storage.memoryFraction", "0.2")
    //fraction of spark.executor.memory which will be used for shuffle
    conf.set("spark.shuffle.memoryFraction", "0.2")
    //If set to "true", consolidates intermediate files created during a shuffle
    conf.set("spark.shuffle.consolidateFiles","true")
    
    conf.set("spark.eventLog.enabled", "false")
    // Help prevent FetchFailed exceptions when single node is heavily taxed
    // http://stackoverflow.com/questions/26247654/spark-fail-to-run-the-terasort-when-the-amount-of-data-gets-bigger
    conf.set("spark.core.connection.ack.wait.timeout", "6000")
    // Attempting to fix "Map output statuses were 10810591 bytes which exceeds spark.akka.frameSize (10485760 bytes)"
    conf.set("spark.akka.frameSize", "1000")
    conf.set("spark.akka.timeout","6000")
    //Size of each piece of a block in kilobytes for TorrentBroadcastFactory.
    conf.set("spark.broadcast.blockSize","10240")
    conf.set("spark.akka.threads","12")
    
    //some other memory config in JVM, e.g. PermGen
    //similar configs for driver should be set in command, e.g. though --driver-java-options or in default properties file
    //conf.set("spark.executor.extraJavaOptions","-XX:MaxPermSize=512m -XX:+CMSClassUnloadingEnabled -XX:+CMSPermGenSweepingEnabled")
    

    val sc = new SparkContext(conf)
    
    //input files on HDFS
    var edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-LiveJournal1.txt"
    //var edgesFileOnHDFS = "/Users/yifanli/Data/edges_test.txt"
    //var landmarksFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/landmarks.txt"
    //var seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top200deg.txt"
    //var seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top16deg.txt"
    //var seedsFileOnHDFS = "/Users/yifanli/Data/onlyHubs_test.txt"
    //var resultFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc_blockpartition"
    //the directory to store the files of edge partitions
    //var parFilePathOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/edgePartitions"
    var prefix = ""
    if(args(5).equals("Pokec")){
      edgesFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/soc-PokecRelationships.txt"
      prefix = args(5)
    }
      
    //partitions number
    val numPartitions = args(0).toInt
    
  //iterations number(the length of inverse P-distance)
  var numIterations = args(3).toInt
  
  //the num of seed file:
  //var seedFiles = Array("16","32","64","100","150","200","300","400")
  var SeedsFile = args(4)
  var seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top"+prefix+SeedsFile+"deg.txt"
  

  //the value for each seed
  val sVal = 100000d
  
  //teleportation constant
  val ct = 0.15
    
  //to indicate the outDegree of each vertex
  val outD:VertexId = -1

  //storage levels
  val MADS = StorageLevel.MEMORY_AND_DISK_SER
  val MAD = StorageLevel.MEMORY_AND_DISK
  
  //top-k values for each edge
  val topk = args(6).toInt
  
  //the number of partitions expected
  val numOfPars = args(7).toInt
  
  //path to store the partitioning result
  val localPathToStorePartitions = "SizeFixed_"+ args(5) +"_"+SeedsFile+"_"+numIterations
  
  //the percentage of edges to be re-scheduled
  val perReS = 0.1

  /*
   * to calculate the top-k max elements from a hashmap
   * NOTICE: only top-k values will be kept.
   */
  def getTopK(k:Int, hm:HashMap[VertexId, Double]):HashMap[VertexId, Double] ={
    val nhm = new HashMap[VertexId, Double]()
    if(hm.isEmpty){
      return nhm
    }
    val z = {if(k>hm.size) hm.size else k}
    for(i <- 1 to z){
      val m = hm.maxBy(_._2)
      nhm += m
      hm -= m._1
    }
    return nhm
  }
  
//a function to divide the values in vertex's hashmap by its out-degree
  def hmCal(srcAtt: (Int,HashMap[VertexId,Double])) : HashMap[VertexId, Double] = {
    val hm = new HashMap[VertexId, Double]()
    //hm ++= srcAtt._2
    srcAtt._2.foreach(p =>{
      //tempHM += (p._1 -> p._2.*(iniScore))
      //println("++++++++++++++++++++")
      //println(p._2)
      hm += ((p._1 -> p._2*(1/srcAtt._1.toDouble)))
      //println("*******************")
      //println(p._2*(1/srcAtt._1.toDouble))
      //println("*******************")
      //println(srcAtt._2.apply(p._1))
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
  //NOTICE: minEdgePartitions --> numEdgePartitions for latest API
  val graph = GraphLoader.edgeListFile(sc, edgesFileOnHDFS, false, numPartitions, edgeStorageLevel = MAD,
     vertexStorageLevel = MAD).partitionBy(PartitionStrategy.EdgePartition2D)
  
  // to load the hub vertices file to construct a hub vertices RDD[(VertexId, Double)]
  //val hubRDD: RDD[(VertexId, Double)] = sc.textFile(seedsFileOnHDFS).coalesce(numPartitions).map(_.split('\t')).map(line => (line(0).trim.toLong, line(1).trim.toDouble))
  val hubRDD: RDD[(VertexId, Double)] = sc.textFile(seedsFileOnHDFS).map(_.split('\t')).map(line => (line(0).trim.toLong, line(1).trim.toDouble))
  
  //val initialVertexRDD = graph.collectNeighborIds(EdgeDirection.Out).map{case(id:VertexId, a:Array[VertexId]) => (id, initialHashMap(a))}.persist(StorageLevel.MEMORY_AND_DISK)
  
  // to get the out degrees of each vertex
  val outDeg: RDD[(VertexId, Int)] = graph.outDegrees
  
  //to construct the vertices RDD by joining with hubs preference values
  val verRDD = graph.vertices.leftJoin(hubRDD)((id, a, b) => a+b.getOrElse(0.0)-1.0).map{case(id:VertexId,v:Double) => (id, (initialHashMap(id,v), initialHashMap(id,v)))}.persist(MAD)
  //val verRDD = graph.vertices.leftJoin(hubRDD)((id, a, b) => a+b.getOrElse(0.0)-1.0).map{case(id:VertexId,v:Double) => (id, (initialHashMap(id,v), initialHashMap(id,v)))}
  
  // note that here we can optimize it using graph.mapVertices()
  // but also need more codes(vertices tables join?) to get neighbors
  //var initialGraph = Graph(initialVertexRDD, graph.edges).persist(StorageLevel.MEMORY_ONLY)
  //val edgeStorageLevel=StorageLevel.MEMORY_AND_DISK
  //var initialGraph = Graph(verRDD3, graph.edges, null,edgeStorageLevel = MAD, vertexStorageLevel = MAD)
  var initialGraph = Graph(verRDD, graph.edges, null,edgeStorageLevel = MAD, vertexStorageLevel = MAD).outerJoinVertices(outDeg){(vid, hmc, optDeg) => (optDeg.getOrElse(0), hmc)}
  
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
    	attr._2._1.update(p._1, p._2 + attr._2._1.getOrElse(p._1, 0.0))
    	//for accurate rank value:
    	//attr._2._1.update(p._1, p._2 + attr._2._1.getOrElse(p._1, 0.0)*ct)
    }
      )
    //to limit the msgSum size
    val smallMsgSum = new HashMap[VertexId, Double]()
    if(msgSum.size > topk){
    	smallMsgSum ++= getTopK(topk,msgSum)
      }else{
        smallMsgSum ++= msgSum
      }
    //to limit the vertex attribute size
    if(attr._2._1.size > topk){
    	return (attr._1, (getTopK(topk, attr._2._1),smallMsgSum))
    }else{
    	return (attr._1, (attr._2._1,smallMsgSum))
    }
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
  //ppr.vertices.saveAsTextFile(resultFilePathOnHDFS)
  
  /*
   * to count how many vertices have NOT been visited by seeds
   */
  val idleVertices = ppr.vertices.filter(v => v._2._2._1.isEmpty)
  println("the num of vertices that has NOT been visited: "+idleVertices.count.toString)
  
  /*
   * to count how many edges without visited vertices
   */
  val numEdgesVisited = ppr.triplets.map(e => if (e.srcAttr._2._1.isEmpty & e.dstAttr._2._1.isEmpty) 1 else 0).sum
  println("the num of edges that has NO visited vertices: " + numEdgesVisited.toString)
  

  //to create a new RDD to store (edge_ids,topk_hashmap) items
  val edgeRdd = ppr.triplets.map(e => ((e.srcId, e.dstId),getTopK(topk, messageCombiner(e.srcAttr._2._1,e.dstAttr._2._1)))).repartition(numPartitions)
  
  /*
   * to group the edges with same top-1 value
   * NOTICE: if the edge's hashmap is empty, it will be allocated to -1 
   */
  //val gs = edgeRdd.map(e => {if(e._2.isEmpty) (-1, e._1) else (e._2.maxBy(_._2)._1, e._1)})
  //val gsGroups = gs.groupByKey(numPartitions)
  //val nullHM = new HashMap[VertexId, Double]()
  var gs = edgeRdd.map(e => {if(e._2.isEmpty) (((-1L, 0.0),HashMap[VertexId, Double]()), e._1) else ((e._2.maxBy(_._2),(e._2 -= e._2.maxBy(_._2)._1)), e._1)})
  var reversedGS = gs.map(x => (x._2, x._1))
  
  //val gs = edgeRdd.map(e => x)
  
  //var gsArrBuf = ArrayBuffer(gs.collect : _*)
  //var gsArrBuf = gs.collect.to[ArrayBuffer]
  //var gsSortedArrBuf = gsArrBuf.sortBy(x => x._1._1._2.toDouble)
  //val zz = gsArr.sortBy(x => x._1._1._2.toDouble)
  

  
  
  //the maximum size of each partition(number of edges included)
  val maxSizeOfPar = gs.count.toInt/hubRDD.count.toInt + 1
  
  //to create, e.g. 16, ArrayBuffers to store those edges to be allocated to each partitions.
  //an extra buffer to store those edges to -1
  var parBufs = new Array[ArrayBuffer[(VertexId, VertexId)]](hubRDD.count.toInt+1)
  //for(x <- parBufs){
  //  x.append((-1L, -1L))
  //}
  
  //a hashmap to store the mapping from seed_ID to each buffer above
  var seedBuffMapping = new HashMap[VertexId, Int]()
  var bufId = 0
  for(x <- hubRDD.toArray){
    seedBuffMapping += (x._1 -> bufId)
    bufId += 1
  }
  seedBuffMapping += (-1L -> bufId)
  
  //to store the edges that will be re-scheduled
  var reSchEdges = new ArrayBuffer[(VertexId, VertexId)]()
  var tt = ArrayBuffer(((0L, 0L),0))
  var reSchEdgesRDD = sc.parallelize(tt)
  
  //to group the edges by maximum value 
  var toGroupGS = gs.map(x => (x._1._1._1,(x._1._1._2, x._2)))
  var groupedGS = toGroupGS.groupByKey()
  var maxSizeOfSchel = (maxSizeOfPar*(1-perReS)).toInt
  //var groupedItr = groupedGS.toLocalIterator
  
  //to get the count of edges of each seed
  var countE = groupedGS.map(x => (x._1, x._2.size))
  
  //to remove the edges to "-1" seed
  //sort them in descending order
  var countEdges = countE.filter(_._1 != -1L).sortBy(x => x._2, false)
  var countEdgesItr = countEdges.toLocalIterator
  
  //an array[arraybuffer] to store the blocks scheduling result
  var blocksArr = new Array[(Int, ArrayBuffer[Long])](numOfPars)
  
  //to initialize the array above
  if(countE.count >= numOfPars){
	  for(i <- 0 to (blocksArr.length-1)){
		 var idSize = countEdgesItr.next
		 var tep = ArrayBuffer(idSize._1)
		 blocksArr(i) = (idSize._2,tep)
		 
	  }
  }else{
    println("Failed to merge blocks! as the blocks is not enough for partitions expected!")
  }
  
  //to schedule the rest of blocks based on their sizes, using 4/3 theorem by R. L. Graham
  while(countEdgesItr.hasNext){
    var idSize = countEdgesItr.next
    //var resortedBlocksArr = blocksArr.sortBy(x => x._1)
    var iSmall = 0
    var sizeSmall = blocksArr(0)._1
    for(i <- 1 to (blocksArr.length-1)){
      if(blocksArr(i)._1 < sizeSmall){
    	  iSmall = i
    	  sizeSmall = blocksArr(i)._1
      }
    }
    blocksArr(iSmall)._2.append(idSize._1)
    blocksArr(iSmall) = ((sizeSmall+idSize._2),blocksArr(iSmall)._2)
    
  }
  
  //to deal with the edges in block "-1"
  var minusOne = groupedGS.filter(_._1 == -1L).first
  var sizeOfMinusOne = minusOne._2.size
  
  //sort the blocks array obtained before 
  //and add an attribute that indicates the number of edges introduced from "-1" block
  var sortedBlocksArr = blocksArr.sortBy(x => x._1).map(x => (x, 0))
  var sumTep = sizeOfMinusOne
  var zt = 0
  while(sumTep > 0){
    zt = zt+1
    println("zt -------------> "+zt.toString)
    
    var i = 0
    var j = 1
    while((j <= sortedBlocksArr.length-1) && ((sortedBlocksArr(j)._1._1 - sortedBlocksArr(i)._1._1) == 0)){
      i = i+1
      j = j+1
    }
    var bal = 0
    if(j <= (sortedBlocksArr.length-1)){
      bal = sortedBlocksArr(j)._1._1 - sortedBlocksArr(i)._1._1
      
      var aver = sumTep/(i+1)
      if(aver < bal){
    	  if(i == 0){
    		  sortedBlocksArr(i) = (((sortedBlocksArr(i)._1._1+aver), sortedBlocksArr(i)._1._2), sortedBlocksArr(i)._2+aver)
    	  }else{
    		  for(t <- 0 to i-1){
    			  sortedBlocksArr(t) = (((sortedBlocksArr(t)._1._1+aver), sortedBlocksArr(t)._1._2), sortedBlocksArr(t)._2+aver)  
    		  }
    		  var rest = sumTep - (aver*i)
    				  sortedBlocksArr(i) = (((sortedBlocksArr(i)._1._1+rest), sortedBlocksArr(i)._1._2), sortedBlocksArr(i)._2+rest)  
    	  }
    	  sumTep = sumTep - sumTep
      }else{
    	  for(t <- 0 to i){
    		  sortedBlocksArr(t) = (((sortedBlocksArr(t)._1._1+bal), sortedBlocksArr(t)._1._2), sortedBlocksArr(t)._2+bal)  
    	  }
    	  sumTep = sumTep - bal*(i+1)
      }
    }else{
      if(j == sortedBlocksArr.length){
        var ba = sumTep/j
        for(t <- 0 to i-1){
        sortedBlocksArr(t) = (((sortedBlocksArr(t)._1._1+ba), sortedBlocksArr(t)._1._2), sortedBlocksArr(t)._2+ba)  
        }
        var rest = sumTep - ba*i
        sortedBlocksArr(i) = (((sortedBlocksArr(i)._1._1+rest), sortedBlocksArr(i)._1._2), sortedBlocksArr(i)._2+rest)
        
        sumTep = 0
        
        
      }
    }
    
    
  
	  sortedBlocksArr = sortedBlocksArr.sortBy(x => x._1._1)
	  
	  println("sumTep -------------> "+sumTep.toString)
	  for(x <- sortedBlocksArr){
	    println(x._1._1)
	  }
	  println("-------------")
  }
  
 
  println("----------- the partitions info: ---------")
  println("to compute the Standard Deviation(SD): ")
  var sumSD = 0L
  var ave = edgeRdd.count/numOfPars
  println("the average number of edges in one partition: "+ave.toString)
  
  for(x <- sortedBlocksArr){
    var numEs = x._1._1
    println("size of partition #:  "+ numEs.toString)
    if(numEs > ave){
      sumSD = sumSD + (numEs-ave)*(numEs-ave)
    }else{
      sumSD = sumSD + (ave-numEs)*(ave-numEs)
    }
  }
  
  var stdDev = math.sqrt(sumSD/numOfPars)
  println("the Standard Deviation(SD): "+stdDev.toString)
  println("the percentage of SD in an average partition: "+(stdDev.toFloat/ave.toFloat).toString)
  
  

  //below temporarily...
    
  //to write the schedule results(union of RDDs) into files
  var edgesFMItr = minusOne._2.toIterator
  for(i <- 0 to (sortedBlocksArr.length-1)){
    var x = sortedBlocksArr(i)
    var sizeOfPar = x._1._1
    var blocksArr = x._1._2
    var edgesFromMinus = x._2
    
    var parRDD = toGroupGS.filter(_._1 == blocksArr.apply(0)).map(x => x._2._2)
    if(blocksArr.length > 1){
    	for(i <- 1 to (blocksArr.length-1)){
    		var tpRDD = toGroupGS.filter(_._1 == blocksArr.apply(i)).map(x => x._2._2)
    		parRDD = parRDD.union(tpRDD)
    	}
    }
    if(edgesFromMinus > 0){
      var numE = edgesFromMinus
      var edgesFM = new ArrayBuffer[(VertexId,VertexId)]()
      while(edgesFMItr.hasNext && (numE>0)){
        edgesFM.append(edgesFMItr.next._2)
        numE = numE-1
      }
      var edgesFMRDD = sc.parallelize(edgesFM)
      parRDD = parRDD.union(edgesFMRDD)
    }
    
    //write parRDD into file
    //val writer = new PrintWriter(new File("GraphPartition_"+"LiveJ"+"_"+sizeOfPar.toString+".txt" ))
    val writer = new PrintWriter(new File("GraphPartition_"+i.toString+"_"+args(5)+"_"+sizeOfPar.toString+".txt" ))
    //for(srcAnddst <- parRDD.toLocalIterator){
    for(srcAnddst <- parRDD.toArray){
      writer.write(srcAnddst._1.toString)
      writer.write("\t")
      writer.write(srcAnddst._2.toString)
      writer.write("\n")
    }
    
    writer.close()
    
    
  }
  

  
  
  println("finished!")
  
  sc.stop()
  
  }

}