package io.honeycomb.common.job;

import io.honeycomb.common.model.ReaderJobConfig;

/**
 * Created by guoyubo on 2018/1/10.
 */
public abstract class AbstractReaderJob implements ReaderJob {

  protected ReaderJobConfig readerJobConfig;

  public void setReaderJobConfig(final ReaderJobConfig readerJobConfig) {
    this.readerJobConfig = readerJobConfig;
  }




}
