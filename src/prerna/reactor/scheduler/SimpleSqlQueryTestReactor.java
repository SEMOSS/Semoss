package prerna.reactor.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import org.quartz.Scheduler;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SimpleSqlQueryTestReactor extends AbstractReactor {

    private static final Logger logger = LogManager.getLogger(SimpleSqlQueryTestReactor.class);

    public static final String DB_ID = "dbId";
    public static final String JOB_NAME = "jobName";
    public static final String JOB_GROUP = "jobGroup";

    public SimpleSqlQueryTestReactor() {
        this.keysToGet = new String[] { DB_ID, JOB_NAME, JOB_GROUP };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        String dbId = this.keyValue.get(DB_ID);
        String jobName = this.keyValue.get(JOB_NAME);
        String jobGroup = this.keyValue.get(JOB_GROUP);

        RDBMSNativeEngine database = (RDBMSNativeEngine) Utility.getDatabase(dbId);
        String sql = "DELETE FROM LARGE_JOBS_2;";
        Connection conn = null;

        try {
            conn = database.getConnection();
            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            	stmt.setQueryTimeout(2000);
                int rows = stmt.executeUpdate();
                logger.info("Deleted {} rows from LARGE_JOBS_2", rows);
            }

            // Check if the job is currently executing before committing
            Scheduler scheduler = SchedulerFactorySingleton.getInstance().getScheduler();
            JobKey jobKey = JobKey.jobKey(jobName, jobGroup);

            boolean isExecuting = false;
            List<JobExecutionContext> executingJobs = scheduler.getCurrentlyExecutingJobs();
            for (JobExecutionContext ctx : executingJobs) {
                if (ctx.getJobDetail().getKey().equals(jobKey)) {
                    isExecuting = true;
                    break;
                }
            }

            if (!isExecuting) {
                logger.info("Job is not currently executing after DB operation. Rolling back and aborting commit.");
                conn.rollback();
                return new NounMetadata("Job not executing after DB operation, operation rolled back", PixelDataType.CONST_STRING);
            }

            conn.commit();
        } catch (Exception e) {
            logger.error("Failed to execute delete: ", e);
            if (conn != null) {
                try { conn.rollback(); } catch (Exception ex) { logger.error("Rollback failed", ex); }
            }
            throw new RuntimeException("Failed to execute delete: " + e.getMessage(), e);
        } finally {
            if (conn != null) {
                try { conn.close(); } catch (Exception e) { logger.error("Connection close failed", e); }
            }
        }

        return new NounMetadata("Delete completed", PixelDataType.CONST_STRING);
    }
}
