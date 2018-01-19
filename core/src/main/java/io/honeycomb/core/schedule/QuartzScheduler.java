/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.honeycomb.core.schedule;

import static java.util.Objects.requireNonNull;

import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleJobFactory;
import org.quartz.spi.JobFactory;
import org.quartz.spi.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.SimpleThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Manages Quartz schedules. Azkaban regards QuartzJob and QuartzTrigger as an one-to-one mapping.
 */
@Service
public class QuartzScheduler {

  //Unless specified, all Quartz jobs's identities comes with the default job name.
  private static final String DEFAULT_JOB_NAME = "quartz-job";
  private static final Logger logger = LoggerFactory.getLogger(QuartzScheduler.class);
  private Scheduler scheduler = null;

  @Autowired
  JobFactory jobFactory;

  @PostConstruct
  public void init() throws SchedulerException {
    DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();
    factory.createScheduler(taskExecutor(), jobStore());

    this.scheduler = factory.getScheduler();

    // Currently Quartz only support internal job schedules. When we migrate to User Production
    // flows, we need to construct a Guice-Free JobFactory for use.
    this.scheduler.setJobFactory(new SimpleJobFactory());

    this.start();
  }

  @PreDestroy
  public void destroy() {
    this.shutdown();
  }

  public JobStore jobStore() {
    JobStore jobStore = new RAMJobStore();
    return jobStore;
  }

  public SimpleThreadPoolTaskExecutor taskExecutor() throws SchedulerConfigException {
    SimpleThreadPoolTaskExecutor taskExecutor = new SimpleThreadPoolTaskExecutor();
    taskExecutor.setInstanceName("dbm-task-executor");
    taskExecutor.setThreadCount(Runtime.getRuntime().availableProcessors() * 16);
    taskExecutor.setWaitForJobsToCompleteOnShutdown(true);
    taskExecutor.afterPropertiesSet();
    return taskExecutor;
  }

  public void start() {
    try {
      this.scheduler.start();
    } catch (final SchedulerException e) {
      logger.error("Error starting Quartz scheduler: ", e);
    }
    logger.info("Quartz Scheduler started.");
  }

  public void cleanup() {
    logger.info("Cleaning up schedules in scheduler");
    try {
      this.scheduler.clear();
    } catch (final SchedulerException e) {
      logger.error("Exception clearing scheduler: ", e);
    }
  }

  public void pause() {
    logger.info("pausing all schedules in Quartz");
    try {
      this.scheduler.pauseAll();
    } catch (final SchedulerException e) {
      logger.error("Exception pausing scheduler: ", e);
    }
  }

  public void resume() {
    logger.info("resuming all schedules in Quartz");
    try {
      this.scheduler.resumeAll();
    } catch (final SchedulerException e) {
      logger.error("Exception resuming scheduler: ", e);
    }
  }

  public void shutdown() {
    logger.info("Shutting down scheduler");
    try {
      this.scheduler.shutdown();
    } catch (final SchedulerException e) {
      logger.error("Exception shutting down scheduler: ", e);
    }
  }

  public void unregisterJob(final String groupName, final String jobName) throws SchedulerException {
    if (!isJobExist(groupName, jobName)) {
      logger.warn("can not find job with " + groupName + " in quartz.");
    } else {
      this.scheduler.deleteJob(new JobKey(jobName, groupName));
    }
  }

  /**
   * Only cron schedule register is supported.
   *
   * @param cronExpression the cron schedule for this job
   * @param jobDescription Regarding QuartzJobDescription#groupName, in order to guarantee no
   * duplicate quartz schedules, we design the naming convention depending on use cases: <ul>
   * <li>User flow schedule: we use {@link JobKey#JobKey} to represent the identity of a
   * flow's schedule. The format follows "$projectID_$flowName" to guarantee no duplicates.
   * <li>Quartz schedule for AZ internal use: the groupName should start with letters, rather than
   * number, which is the first case.</ul>
   */
  public void registerJob(final String cronExpression, final QuartzJobDescription jobDescription)
      throws SchedulerException {

    requireNonNull(jobDescription, "jobDescription is null");

    // Not allowed to register duplicate job name.
    if (isJobExist(jobDescription.getGroupName(), jobDescription.getJobName())) {
      throw new SchedulerException(
          "can not register existing job group:" + jobDescription.getGroupName() + " job:" + jobDescription.getJobName());
    }

    if (!CronExpression.isValidExpression(cronExpression)) {
      throw new SchedulerException(
          "The cron expression string <" + cronExpression + "> is not valid.");
    }

    // TODO kunkun-tang: we will modify this when we start supporting multi schedules per flow.
    final JobDetail job = JobBuilder.newJob(jobDescription.getJobClass())
                                    .withIdentity(jobDescription.getJobName(), jobDescription.getGroupName()).build();

    // Add external dependencies to Job Data Map.
    job.getJobDataMap().putAll(jobDescription.getContextMap());

    // TODO kunkun-tang: Need management code to deal with different misfire policy
    final Trigger trigger = TriggerBuilder
        .newTrigger()
        .withSchedule(
            CronScheduleBuilder.cronSchedule(cronExpression)
                .withMisfireHandlingInstructionFireAndProceed()
//            .withMisfireHandlingInstructionDoNothing()
//            .withMisfireHandlingInstructionIgnoreMisfires()
        )
        .build();

    this.scheduler.scheduleJob(job, trigger);
    logger.info("Quartz Schedule with jobDetail " + job.getDescription() + " is registered.");
  }

//
//  public boolean ifJobExist(final String groupName, final String jobName) throws SchedulerException {
//    KeyMatcher<JobKey> keyMatcher = KeyMatcher.keyEquals(new JobKey(jobName, groupName));
//    final Set<JobKey> jobKeySet = isJobExist(groupName, jobName);
//    return jobKeySet != null && jobKeySet.size() > 0;
//  }

  public boolean isJobExist(final String groupName, final String jobName) throws SchedulerException {
    return this.scheduler.checkExists(new JobKey(jobName, groupName));
  }

  public Scheduler getScheduler() {
    return this.scheduler;
  }
}
