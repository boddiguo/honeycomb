package io.honeycomb.reader.datax;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.SecretUtil;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.google.common.base.Stopwatch;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by guoyubo on 2017/10/26.
 */
public class DataXEngine {

  private Logger logger = LoggerFactory.getLogger(DataXEngine.class);


  public static DataXEngine instance = new DataXEngine();

  private DataXEngine() {

    CoreConstant.DATAX_HOME = this.getClass().getClassLoader().getResource(".").getFile();
//    CoreConstant.DATAX_HOME = "/Users/guoyubo/workspace/honeycomb/jdbcreader/src/main/resources";
    CoreConstant.DATAX_CONF_PATH = StringUtils.join(new String[]{
        CoreConstant.DATAX_HOME, "conf", "core.json"}, File.separator);

    CoreConstant.DATAX_CONF_LOG_PATH = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "conf", "logback.xml"}, File.separator);

    CoreConstant.DATAX_PLUGIN_HOME = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "plugin"}, File.separator);

    CoreConstant.DATAX_PLUGIN_READER_HOME = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "plugin", "reader"}, File.separator);

    CoreConstant.DATAX_PLUGIN_WRITER_HOME = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "plugin", "writer"}, File.separator);

    CoreConstant.DATAX_BIN_HOME = StringUtils.join(new String[]{
        CoreConstant.DATAX_HOME, "bin"}, File.separator);

    CoreConstant.DATAX_JOB_HOME = StringUtils.join(new String[]{
        CoreConstant.DATAX_HOME, "job"}, File.separator);

    CoreConstant.DATAX_SECRET_PATH = StringUtils.join(new String[]{
        CoreConstant.DATAX_HOME, "conf", ".secret.properties"}, File.separator);
    CoreConstant.DATAX_STORAGE_TRANSFORMER_HOME = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "local_storage", "transformer"}, File.separator);

    CoreConstant.DATAX_STORAGE_PLUGIN_READ_HOME = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "local_storage", "plugin", "reader"}, File.separator);

    CoreConstant.DATAX_STORAGE_PLUGIN_WRITER_HOME = StringUtils.join(
        new String[]{CoreConstant.DATAX_HOME, "local_storage", "plugin", "writer"}, File.separator);

  }

  public String execute(final String jobContent, final long jobId) {
    Stopwatch stopwatch = Stopwatch.createStarted();
    logger.info("start execute [" + jobContent + "]");
    String result = null;
    try {

      Configuration configuration = parse(jobContent);

      configuration.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);

      //打印vmInfo
      VMInfo vmInfo = VMInfo.getVmInfo();
      logger.info(vmInfo.toString());

      ConfigurationValidate.doValidate(configuration);
      Engine engine = new Engine();
      Engine.setRuntimeMode("standalone");
      result = engine.start(configuration);

    } finally {
      stopwatch.stop();
      logger.info("end execute [" + jobContent + "]" + "\n and spend time is " + stopwatch.elapsed(TimeUnit.SECONDS));
    }
    return result;
  }

  /**
   * 指定Job配置路径，ConfigParser会解析Job、Plugin、Core全部信息，并以Configuration返回
   */
  private Configuration parse(final String jobContent) {
    Configuration configuration = Configuration.from(jobContent);
    configuration = SecretUtil.decryptSecretKey(configuration);

    configuration.merge( Configuration.from(new File(CoreConstant.DATAX_CONF_PATH)), false);
    // todo config优化，只捕获需要的plugin
    String readerPluginName = configuration.getString(
        CoreConstant.DATAX_JOB_CONTENT_READER_NAME);
    String writerPluginName = configuration.getString(
        CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);

    String preHandlerName = configuration.getString(
        CoreConstant.DATAX_JOB_PREHANDLER_PLUGINNAME);

    String postHandlerName = configuration.getString(
        CoreConstant.DATAX_JOB_POSTHANDLER_PLUGINNAME);

    Set<String> pluginList = new HashSet<String>();
    pluginList.add(readerPluginName);
    pluginList.add(writerPluginName);

    if(StringUtils.isNotEmpty(preHandlerName)) {
      pluginList.add(preHandlerName);
    }
    if(StringUtils.isNotEmpty(postHandlerName)) {
      pluginList.add(postHandlerName);
    }
    try {
      configuration.merge(ConfigParser.parsePluginConfig(new ArrayList<String>(pluginList)), false);
    }catch (Exception e){
      //吞掉异常，保持log干净。这里message足够。
      logger.warn(String.format("插件[%s,%s]加载失败，1s后重试... Exception:%s ", readerPluginName, writerPluginName, e.getMessage()));
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        //
      }
      configuration.merge(ConfigParser.parsePluginConfig(new ArrayList<String>(pluginList)), false);
    }

    return configuration;
  }




}
