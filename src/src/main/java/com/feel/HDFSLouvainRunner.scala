package com.feel

/**
 * Created by canoe on 6/24/15.
 */
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom
import scala.util.Random.nextInt


/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 *
 * See LouvainHarness for algorithm details
 */
class HDFSLouvainRunner(minProgress:Int,progressCounter:Int,outputdir:String) extends LouvainHarness(minProgress:Int,progressCounter:Int){

  var qValues = Array[(Int,Double)]()

  override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
    val nextIntFlag = nextInt()
    graph.vertices.saveAsTextFile(outputdir+"/level_"+level + "_" + nextIntFlag + "_vertices")
    graph.edges.saveAsTextFile(outputdir+"/level_"+level + "_" + nextIntFlag + "_edges")
    //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
    //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")
    qValues = qValues :+ ((level,q))
    println(s"qValue: $q")

    // overwrite the q values at each level
    sc.parallelize(qValues, 1).saveAsTextFile(outputdir + "level_" + level + "_" + nextIntFlag + "/qvalues")
  }

  override def finalSave(sc: SparkContext, level: Int, q: Double, graph:Graph[VertexState, Long]) = {
    val nextIntFlag = nextInt()
    graph.vertices.saveAsTextFile(outputdir+"/final_"+level + "_" + nextIntFlag + "_vertices")
    graph.edges.saveAsTextFile(outputdir+"/final_"+level + "_" + nextIntFlag + "_edges")
    //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
    //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")

  }
}
