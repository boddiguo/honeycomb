package com.boddi.honeycomb.sparkbee.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;


import java.io.IOException;

/**
 * Created by guoyubo on 2017/7/17.
 */
public class HadoopClient {

  private static JobConf conf;

  public static synchronized JobConf getConf() {
    System.setProperty("java.security.krb5.conf", HadoopClient.class.getClassLoader().getResource("krb5.conf").getPath());
    if (conf != null) {
      return conf;
    } else {
      try {


        Configuration configuration = new Configuration();
//        configuration.set("hadoop.security.authentication", "Kerberos");
//        List<String> confFiles = getHadoopConfFiles();
//        for (String confFile : confFiles) {
//          configuration.addResource(new Path(confFile));
//        }

        conf = new JobConf(configuration);
        return conf;


      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }
  }



  public static FileSystem getFileSystem() {
    try {
      return FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }



  public static void mkdirs( String pathName) {

    try {
      Path path;

      path = new Path(pathName);

      final FileSystem hdfs = getFileSystem();

      if (!hdfs.exists(path)) {

        hdfs.mkdirs(path);
      }


    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }




  public static boolean exist( String pathName) {

    try {
      Path path;

      path = new Path(pathName);

      final FileSystem hdfs = getFileSystem();

      if (hdfs.exists(path)) {
       return true;
      }


    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return false;
  }


  public static void main(String[] args) {
    HadoopClient.mkdirs("/tmp/test");
  }





}
