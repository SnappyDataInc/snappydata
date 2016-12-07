/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.spark.util

import java.io.File
import java.net.URL
import java.security.SecureClassLoader

import _root_.io.snappydata.Constant
import com.pivotal.gemfirexd.internal.engine.Misc
import spark.jobserver.util.ContextURLClassLoader

import org.apache.spark.SparkContext

object SnappyUtils {

  def getSnappyStoreContextLoader(parent: ClassLoader): ClassLoader = parent match {
    case _: SnappyContextLoader => parent // no double wrap
    case _ => new SnappyContextLoader(parent)
  }

  def removeJobJar(sc: SparkContext): Unit = {
    def getName(path: String): String = new File(path).getName
    val jobJarToRemove = sc.getLocalProperty(Constant.JOB_SERVER_JAR_NAME)
    val keyToRemove = sc.listJars().filter(getName(_) == getName(jobJarToRemove))
    if (keyToRemove.nonEmpty) {
      sc.addedJars.remove(keyToRemove.head)
    }
  }

  def getSnappyContextURLClassLoader(
      parent: ContextURLClassLoader): ContextURLClassLoader = parent match {
    case _: SnappyContextURLLoader => parent // no double wrap
    case _ => new SnappyContextURLLoader(parent)
  }
}

class SnappyContextLoader(parent: ClassLoader)
    extends SecureClassLoader(parent) {

  @throws(classOf[ClassNotFoundException])
  override def loadClass(name: String): Class[_] = {
    try {
      parent.loadClass(name)
    } catch {
      case cnfe: ClassNotFoundException =>
        Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    }
  }
}

class SnappyContextURLLoader(parent: ClassLoader)
    extends ContextURLClassLoader(Array[URL](), parent) {

  @throws(classOf[ClassNotFoundException])
  override def loadClass(name: String): Class[_] = {
    try {
      super.loadClass(name)
    } catch {
      case cnfe: ClassNotFoundException =>
        Misc.getMemStore.getDatabase.getClassFactory.loadClassFromDB(name)
    }
  }
}
