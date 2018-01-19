package io.honeycomb.common.job;

/**
 * Created by guoyubo on 2018/1/10.
 */
public interface ReaderJob {


  void init();
  void pre();
  boolean run();
  boolean cancel();
  void post();
  void destroy();

}
