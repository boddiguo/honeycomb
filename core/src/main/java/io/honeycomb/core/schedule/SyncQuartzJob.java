package io.honeycomb.core.schedule;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import io.honeycomb.common.model.ReaderJobConfig;
import io.honeycomb.core.engine.JobEngine;

/**
 * Created by guoyubo on 2017/10/27.
 */
@DisallowConcurrentExecution
public class SyncQuartzJob extends AbstractQuartzJob {


  public static final String JOBDATAMAP_BATCHJOB = "JOBDATAMAP_BATCHJOB";
  public static final String JOBDATAMAP_BATCH_MANAGE = "JOBDATAMAP_SYNC_MANAGE";
  public static final String JOBDATAMAP_LOGSERVICE = "JOBDATAMAP_LOGSERVICE";


  @Override
  public void execute(final JobExecutionContext context) throws JobExecutionException {
    JobDataMap dataMap = context.getMergedJobDataMap();
    ReaderJobConfig readerJobConfig = (ReaderJobConfig) dataMap.get(JOBDATAMAP_BATCHJOB);

    JobEngine jobEngine = (JobEngine) dataMap.get(JOBDATAMAP_BATCH_MANAGE);
    jobEngine.execute(readerJobConfig, true);
  }
}
