package com.sohu.utils

import unfiltered.util.Port
import com.sohu.utils.UnfilteredWebApp.Arguments

/**
 * build up a little web app that serves static files from the resource directory
 * and other stuff from the provided plan
 * User: pierre
 * Date: 10/3/13
 */
trait UnfilteredWebApp[T <: Arguments] {

  def htmlRoot: String

  def setup(args: T): unfiltered.filter.Plan

  def afterStart() {}

  def afterStop() {}
  
  def start(parsed: T) {
    val root = getClass.getResource("/offsetapp")
    println("serving resources from: " + root)
    unfiltered.jetty.Http(parsed.port)
      .resources(root) //whatever is not matched by our filter will be served from the resources folder (html, css, ...)
      .filter(setup(parsed))
      .run(_ => afterStart(), _ => afterStop())
  }
}

object UnfilteredWebApp {
  
  trait Arguments {
    var port = Port.any //端口参数（--port xxx），定义任意的端口
  }
  
}
