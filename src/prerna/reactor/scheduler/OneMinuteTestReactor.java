package prerna.reactor.scheduler;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.matchers.GroupMatcher;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OneMinuteTestReactor extends AbstractReactor {

    public OneMinuteTestReactor() { }

    @Override
    public NounMetadata execute() {

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        StringBuilder log = new StringBuilder();
        log.append("Task started...\n");
        log.append(getSchedulerData());

        // Schedule a task to run after 1 minute
        scheduler.schedule(() -> {
            log.append("Timeout reached! Task completed.\n");
            log.append(getSchedulerData());
            scheduler.shutdown();
        }, 1, TimeUnit.MINUTES);

        try {
            Thread.sleep(30000); // Simulate 30 seconds of work
            log.append("Work done before timeout.\n");
            log.append(getSchedulerData());
        } catch (InterruptedException e) {
            log.append("InterruptedException: ").append(e.getMessage()).append("\n");
        }

        try {
            scheduler.awaitTermination(70, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.append("Scheduler awaitTermination interrupted: ").append(e.getMessage()).append("\n");
        }

        return new NounMetadata(log.toString(), PixelDataType.CONST_STRING);
    }

    private String getSchedulerData() {
        StringBuilder sb = new StringBuilder();
        try {
            Scheduler quartzScheduler = SchedulerFactorySingleton.getInstance().getScheduler();
            sb.append("---- Scheduler Data ----\n");

            for (String groupName : quartzScheduler.getJobGroupNames()) {
                sb.append("Job Group: ").append(groupName).append("\n");
                for (JobKey jobKey : quartzScheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    sb.append("  Job: ").append(jobKey).append("\n");
                    List<? extends Trigger> triggers = quartzScheduler.getTriggersOfJob(jobKey);
                    for (Trigger trigger : triggers) {
                        Trigger.TriggerState triggerState = quartzScheduler.getTriggerState(trigger.getKey());
                        sb.append("    Trigger: ").append(trigger.getKey())
                          .append(" State: ").append(triggerState).append("\n");
                    }
                }
            }

            List<JobExecutionContext> executingJobs = quartzScheduler.getCurrentlyExecutingJobs();
            for (JobExecutionContext context : executingJobs) {
                sb.append("Currently executing job: ")
                  .append(context.getJobDetail().getKey()).append("\n");
            }

            sb.append("Scheduler Metadata: ")
              .append(quartzScheduler.getMetaData().toString()).append("\n");
            sb.append("------------------------\n");
        } catch (SchedulerException e) {
            sb.append("Error retrieving scheduler data: ").append(e.getMessage()).append("\n");
        }
        return sb.toString();
    }
}
