package tupol.sparx

import org.apache.hadoop.conf.{ Configuration => HadoopConfiguration }
import org.apache.hadoop.fs.{ Path, FileSystem }

/**
 *
 */
package object benchmarks {

  /**
   * Run a block and return the block result and the runtime in millis
   *
   * @param block
   * @return
   */
  def timeCode[T](block: => T): (T, Long) = {
    val start = new java.util.Date
    val result = block
    val runtime = (new java.util.Date).toInstant.toEpochMilli - start.toInstant.toEpochMilli
    (result, runtime)
  }

  def removeHdfsFile(path: String) = {
    val hdfs = FileSystem.get(new HadoopConfiguration())
    val workingPath = new Path(path)
    hdfs.delete(workingPath, true) // delete recursively
  }
}
