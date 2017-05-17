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
 * 2) seed vertices(e.g. Top300outdeg)
 * 3) the value of seed will be propagated evenly to its neighbors for numIterations iterations
 * 4) the size of each block(more precisely, say Partition) has been fixed.
 * 
 * Usage:
 * => ./bin/spark-submit --class BlockedPartition ~/BlockPartition_v1.jar [partitions] [spark.executor.memory] [spark.cores.max] [iterations] [num of seed file] [Pokec] [topk] [pathToStorePartitions]
 * e.g.
 * nohup /usr/local/spark1/spark-1.4.1-bin-hadoop1/bin/spark-submit --driver-memory 30G --class "BlockedPartition_fixedBlockSize" ~/graphx-montecarlo-fixedsize_blockedpartition_2.10-0.1.jar 300 30G 300 8 0 LiveJournal 16 partitions &
 * nohup /usr/local/spark1/spark-1.4.1-bin-hadoop1/bin/spark-submit --class "BlockedPartition_fixedBlockSize" ~/graphx-montecarlo-fixedsize_blockedpartition_2.10-0.1.jar 300 20G 300 8 0 LiveJournal 16 partitions &
 */

object BlockedPartition_fixedBlockSize {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("BlockPartition-fixedSize_Yifan")
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
    
    //some other memory config in JVM, e.g. PermGen
    //similar configs for driver should be set in command, e.g. though --driver-java-options or in default properties file
    conf.set("spark.executor.extraJavaOptions","-XX:MaxPermSize=512m -XX:+CMSClassUnloadingEnabled -XX:+CMSPermGenSweepingEnabled")
    

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
  var seedFiles = Array("16","32","64","100","150","200","300","400")
  var numSeedFile = args(4).toInt
  var seedsFileOnHDFS = "hdfs://small1-tap1.common.lip6.fr:54310/user/yifan/Top"+prefix+seedFiles(numSeedFile).toString()+"deg.txt"
  

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
  
  //path to store the partitioning result
  val localPathToStorePartitions = "SizeFixed_"+ args(5) +"_"+numSeedFile+"_"+numIterations

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
  val gs = edgeRdd.map(e => {if(e._2.isEmpty) (((-1L, 0.0),HashMap[VertexId, Double]()), e._1) else ((e._2.maxBy(_._2),(e._2 -= e._2.maxBy(_._2)._1)), e._1)})
  //var gsArrBuf = ArrayBuffer(gs.collect : _*)
  //var gsArrBuf = gs.collect.to[ArrayBuffer]
  //var gsSortedArrBuf = gsArrBuf.sortBy(x => x._1._1._2.toDouble)
  //val zz = gsArr.sortBy(x => x._1._1._2.toDouble)
  
  //to sort the edges RDD in descending order on the maximum value in edge's hashmap.
  //NOTICE: the largest partition in this RDD should not be bigger than driver's memory.
  var gsSorted = gs.sortBy(x => x._1._1._2, false)
  
  
  //the maximum size of each partition(number of edges included)
  val maxSizeOfPar = gs.count/hubRDD.count.toInt + 1
  
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
  
  
  //to return an iterator that contains all of the elements in this edges RDD.
  val gsItr = gsSorted.toLocalIterator
  
  //to store those no-where-to-be-placed edges in a TreeSet in ascending order
  //take the largest one using tempTS.last
  var tempTS = new TreeSet[(((VertexId, Double),HashMap[VertexId, Double]),(VertexId, VertexId))]()(Ordering.by(_._1._1.swap))
  
  
  /*
   * to scan the edges RDD
   */
  
  //to count the processed edges in gsItr
  var ecounter = 0


  while(gsItr.hasNext){
    var edgeWithHM = gsItr.next
    ecounter = ecounter+1
    //to indicate if the current edge edgeWithHM has been processed in the end.
    var hasPro = false

    val seedID = edgeWithHM._1._1._1
    
    if(tempTS.isEmpty || (tempTS.last._1._1._2 <= edgeWithHM._1._1._2)){
      if(seedID == -1L){
        if(parBufs(seedBuffMapping.apply(seedID))==null){
    		  var tep = ArrayBuffer(edgeWithHM._2)
    		  parBufs(seedBuffMapping.apply(seedID)) = tep
    		  hasPro = true
    		}else{
    		  parBufs(seedBuffMapping.apply(seedID)).append(edgeWithHM._2)
    		  hasPro = true
    		}
      }else{
    	if(parBufs(seedBuffMapping.apply(seedID))==null || parBufs(seedBuffMapping.apply(seedID)).size < maxSizeOfPar){
    		if(parBufs(seedBuffMapping.apply(seedID))==null){
    		  var tep = ArrayBuffer(edgeWithHM._2)
    		  parBufs(seedBuffMapping.apply(seedID)) = tep
    		  hasPro = true
    		}else{
    		  parBufs(seedBuffMapping.apply(seedID)).append(edgeWithHM._2)
    		  hasPro = true
    		}
    	}else{
    		var edge = edgeWithHM._2
    		
    		if(edgeWithHM._1._2.isEmpty){
    		  tempTS += ((((-1L, 0.0), HashMap[VertexId, Double]()),edge) )
    		}else{
    		  var newMax = edgeWithHM._1._2.maxBy(_._2)
    		  tempTS += (((newMax, (edgeWithHM._1._2 -= newMax._1)),edge) )
    		}
    		hasPro = true
    		
    	}
    }
    }else{
      //val maxE = tempTS.last
      while(!tempTS.isEmpty && (tempTS.last._1._1._2 > edgeWithHM._1._1._2)){
        var maxE = tempTS.last
        var sID = maxE._1._1._1
        if(sID == -1L){
        if(parBufs(seedBuffMapping.apply(sID))==null){
    		  var tep = ArrayBuffer(maxE._2)
    		  parBufs(seedBuffMapping.apply(sID)) = tep
    		  
    		}else{
    		  parBufs(seedBuffMapping.apply(sID)).append(maxE._2)
    		  
    		}
        tempTS -= maxE
      }else{
        if(parBufs(seedBuffMapping.apply(sID))==null || parBufs(seedBuffMapping.apply(sID)).size < maxSizeOfPar){
          //parBufs(seedBuffMapping.apply(sID)).append(maxE._2)
          tempTS -= maxE
          if(parBufs(seedBuffMapping.apply(sID))==null){
    		  var tep = ArrayBuffer(maxE._2)
    		  parBufs(seedBuffMapping.apply(sID)) = tep
    		  
    		}else{
    		  parBufs(seedBuffMapping.apply(sID)).append(maxE._2)
    		  
    		}
          
          
        }else{
          var ed = maxE._2
          
          tempTS -= maxE
          if(maxE._1._2.isEmpty){
            tempTS += ((((-1L, 0.0), HashMap[VertexId, Double]()), ed) )
          }else{
        	var newM = maxE._1._2.maxBy(_._2)
            tempTS += (((newM,(maxE._1._2 -= newM._1)),ed))
          }
        }
      }
      }
      
      
      
    }
    
    
    
    if(!hasPro){
        
      if(seedID == -1L){
        if(parBufs(seedBuffMapping.apply(seedID))==null){
    		  var tep = ArrayBuffer(edgeWithHM._2)
    		  parBufs(seedBuffMapping.apply(seedID)) = tep
    		  hasPro = true
    		}else{
    		  parBufs(seedBuffMapping.apply(seedID)).append(edgeWithHM._2)
    		  hasPro = true
    		}
      }else{
    	  if(parBufs(seedBuffMapping.apply(seedID))==null || parBufs(seedBuffMapping.apply(seedID)).size < maxSizeOfPar){
    		  //parBufs(seedBuffMapping.apply(seedID)).append(edgeWithHM._2)
    		  //hasPro = true
    	    if(parBufs(seedBuffMapping.apply(seedID))==null){
    		  var tep = ArrayBuffer(edgeWithHM._2)
    		  parBufs(seedBuffMapping.apply(seedID)) = tep
    		  hasPro = true
    		}else{
    		  parBufs(seedBuffMapping.apply(seedID)).append(edgeWithHM._2)
    		  hasPro = true
    		}
    	  }else{
    		  var edge = edgeWithHM._2
    		  if(edgeWithHM._1._2.isEmpty){
    		  tempTS += ((((-1L, 0.0), HashMap[VertexId, Double]()),edge) )
    		  }else{
    			  var newMax = edgeWithHM._1._2.maxBy(_._2)
    			  tempTS += (((newMax, (edgeWithHM._1._2 -= newMax._1)),edge) )
    		  }
    		  hasPro = true
    		
    	  }
        
    	}
        
      
      }
    
    
    if(ecounter%1000 == 0){
      println("###################counting....#################")
      println(ecounter)
    }
    
    
    
    
  }
 
  /*
   * to assign the edges left in TreeSet
   */
  if(!tempTS.isEmpty){
    while(!tempTS.isEmpty){
      var maxE = tempTS.last
        var sID = maxE._1._1._1
        if(sID == -1L){
        parBufs(seedBuffMapping.apply(sID)).append(maxE._2)
      }else{
        if(parBufs(seedBuffMapping.apply(sID)).size < maxSizeOfPar){
          parBufs(seedBuffMapping.apply(sID)).append(maxE._2)
          tempTS -= maxE
        }else{
          var ed = maxE._2
          
          tempTS -= maxE
          if(maxE._1._2.isEmpty){
            tempTS += ((((-1L, 0.0), HashMap[VertexId, Double]()), ed) )
          }else{
        	var newM = maxE._1._2.maxBy(_._2)
            tempTS += (((newM,(maxE._1._2 -= newM._1)),ed))
          }
        }
      }
      
      
    }
  }
  
  /*
   * to merge the edges in "-1" partition into other normal partitions
   */
  if(!parBufs(seedBuffMapping.apply(-1L)).isEmpty){
    val sizeOfCan = parBufs(seedBuffMapping.apply(-1L)).size
    for(i <- 0 to (bufId-1) ){
      var vacant = maxSizeOfPar - parBufs(i).size
      if(vacant > 0){
        if(sizeOfCan < vacant){
          parBufs(i) ++= parBufs(seedBuffMapping.apply(-1L))
        }else{
          var parts = parBufs(seedBuffMapping.apply(-1L)).splitAt(vacant.toInt)
          parBufs(i) ++= parts._1
          parBufs(seedBuffMapping.apply(-1L)).clear
          parBufs(seedBuffMapping.apply(-1L)) ++= parts._2
        }
        
      }
      
    }
  }
  
  
  
  /*
   * to write the edges from parBufs[0 ~ bufId-1] into files
   */
  //val seedsWithValue = hubRDD.toArray
  for(x <- hubRDD.toArray){
    var stringedPar = parBufs(seedBuffMapping.apply(x._1)).map(x => x._1.toString+"\t"+x._2.toString)
    var parRDD = sc.parallelize(stringedPar, 1)
    parRDD.saveAsTextFile(localPathToStorePartitions)
    
    //val temp = gsGroups.filter{case(sid,edges) => sid ==x._1}
    //temp.saveAsTextFile(parFilePathOnHDFS)
    
    //val writer = new PrintWriter(new File("GraphPartition_"+x._1.toString+".txt" ))
    //for(srcAnddst <- parBufs(seedBuffMapping.apply(x._1)).toIterable){
    //  writer.write(srcAnddst._1.toString)
    //  writer.write("\t")
    //  writer.write(srcAnddst._2.toString)
    //  writer.write("\n")
    //}
    
    //writer.close()
  }
  //others.saveAsTextFile(parFilePathOnHDFS)
  

  
  
  println("finished!")
  
  sc.stop()
  
  }

}