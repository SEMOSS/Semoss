package prerna.sablecc2.jobs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import prerna.reactor.project.dev_environment.IdleTimeoutReactor;

public class IdleDevEnvironmentJob implements Job {

    private static final Logger classLogger = LogManager.getLogger(IdleDevEnvironmentJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            IdleTimeoutReactor reactor = new IdleTimeoutReactor();
            reactor.execute();
        } catch (Exception e) {
            classLogger.error("Error executing IdleTimeoutReactor", e);
            throw new JobExecutionException("Failed to execute IdleTimeoutReactor", e);
        }
    }
}
