package io.honeycomb.core.engine;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import io.honeycomb.common.job.AbstractReaderJob;
import io.honeycomb.common.model.ReaderJobConfig;
import io.honeycomb.core.config.ReaderJobProperties;

/**
 * Created by guoyubo on 2018/1/16.
 */
@Component
public class JobEngine {

  @Autowired
  ReaderJobProperties readerJobProperties;


  public void execute(ReaderJobConfig readerJobConfig, final boolean isAuto) {

    String readerJobClassName = readerJobProperties.getTypes().get(readerJobConfig.getType());
    AbstractReaderJob readerJob = null;
    try {
      Class<AbstractReaderJob> readerJobClass =
          (Class<AbstractReaderJob>) ClassUtils.forName(readerJobClassName, this.getClass().getClassLoader());
      readerJob = readerJobClass.newInstance();
      readerJob.setReaderJobConfig(readerJobConfig);
      readerJob.init();
      readerJob.pre();
      readerJob.run();
      readerJob.post();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } finally {
      readerJob.destroy();
    }
  }

}
