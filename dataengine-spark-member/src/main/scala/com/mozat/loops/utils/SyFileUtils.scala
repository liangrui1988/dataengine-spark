package com.dataengine.spark.utils

import com.dataengine.spark.core.utils.CFileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object SyFileUtils {

  /**
   * 合并临时文件，需要转GBK,压zip
   *
   * @param srcPath
   * @param dstPath
   * @param filename
   * @return
   */
  def merge(srcPath: String, dstPath: String, filename: String, headers: String) {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    // the "true" setting deletes the source files once they are merged into the new output
    val fiilePath: String = CFileUtils.copyMerge2(hdfs, new Path(srcPath), true, hadoopConfig, headers, filename)
    //处理好后，再次上传hdfs
    hdfs.copyFromLocalFile(true, true, new Path(fiilePath), new Path(dstPath))
    //    new File(fiilePath).delete()
  }

}
