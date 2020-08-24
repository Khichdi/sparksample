package com.jobdox.examples.spark

import java.net.URI

object Util {

  def getClassFilePath(filename: String): String = {
    val res = getClass.getClassLoader.getResource(filename)
    val x = res.getPath
    if (res == null) {
      null
    } else {
      // avoid url encoding of the path
      new URI(res.toString).getPath
    }
  }

}
