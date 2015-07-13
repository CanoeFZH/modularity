package com.feel

/**
 * Created by canoe on 6/24/15.
 */
/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 */
class VertexState extends Serializable{

  var community = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L  // self edges
  var nodeWeight = 0L;  //out degree
  var changed = false

  override def toString(): String = {
    "{community:"+community+",communitySigmaTot:"+communitySigmaTot+
      ",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
  }
}
