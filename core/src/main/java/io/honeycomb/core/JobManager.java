package io.honeycomb.core;

import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import javax.annotation.PostConstruct;

import java.util.List;

import io.honeycomb.common.model.ReaderJobConfig;
import io.honeycomb.core.engine.JobEngine;
import io.honeycomb.core.entity.ReaderItem;
import io.honeycomb.core.entity.ReaderSource;
import io.honeycomb.core.schedule.QuartzJobDescription;
import io.honeycomb.core.schedule.SyncQuartzJob;
import io.honeycomb.core.schedule.QuartzScheduler;
import io.honeycomb.core.service.ReaderSourceService;
import io.honeycomb.core.service.ReaderItemService;

/**
 * Created by guoyubo on 2018/1/10.
 */
@Component
public class JobManager {


  private static Logger logger = LoggerFactory.getLogger(JobManager.class);

  @Autowired
  ReaderSourceService readerSourceService;

  @Autowired
  ReaderItemService readerItemService;


  @Autowired
  QuartzScheduler quartzScheduler;

  @Autowired
  JobEngine jobEngine;

//  @PostConstruct
  public void init() {
    List<ReaderItem> readerItems = readerItemService.getAllReaderItems();
    for (ReaderItem readerItem : readerItems) {
      registerReaderJob(readerItem);
    }
  }


  public void registerReaderJob(final ReaderItem readerItem) {
    ReaderSource readerSource = readerSourceService.getReaderSourceById(readerItem.getSourceId());
    ReaderJobConfig readerJobConfig = new ReaderJobConfig();
    readerJobConfig.setName(readerItem.getName());
    readerJobConfig.setType(readerSource.getType());
    readerJobConfig.setItemConfig(readerItem.getItemConfig());
    readerJobConfig.setSourceConfig(readerSource.getConfig());
    readerJobConfig.setKafkaServerAddress(readerSource.getKafkaServerAddress());

    if (readerSource.getType().equals("JDBC")) {
      String itemConfig = readerItem.getItemConfig();
      JSONObject itemConfigJson = JSON.parseObject(itemConfig);
      String cronExpression = itemConfigJson.getString("cronExpression");
      if (cronExpression != null) {

        final QuartzJobDescription jobDescription = new QuartzJobDescription("db_sync", SyncQuartzJob.class, "etl");

        jobDescription.getContextMap().put(SyncQuartzJob.JOBDATAMAP_BATCHJOB, readerJobConfig);
        jobDescription.getContextMap().put(SyncQuartzJob.JOBDATAMAP_BATCH_MANAGE, jobEngine);
        try {
          quartzScheduler.registerJob(cronExpression, jobDescription);
        } catch (SchedulerException e) {
          e.printStackTrace();
        }

      }
    }
  }


}
