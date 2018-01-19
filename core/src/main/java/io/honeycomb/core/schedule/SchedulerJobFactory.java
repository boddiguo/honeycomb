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

import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.simpl.SimpleJobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.stereotype.Service;

/**
 * Produce Guice-able Job in this custom defined Job Factory.
 *
 * In order to allow Quaratz jobs easily inject dependency, we create this factory. Every Quartz job
 * will be constructed by newJob method.
 */
@Service
public class SchedulerJobFactory extends SimpleJobFactory {

  @Override
  public Job newJob(final TriggerFiredBundle bundle, final Scheduler scheduler)
      throws SchedulerException {
    return super.newJob(bundle, scheduler);
  }
}
