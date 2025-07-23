package prerna.rpa.quartz.jobs.insight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.async.methods.SimpleBody;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequests;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.cookie.BasicCookieStore;
import org.apache.hc.client5.http.cookie.CookieStore;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigKeys;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunPixelJobFromDB implements InterruptableJob {

    private static final Logger logger = LogManager.getLogger(RunPixelJobFromDB.class);
    private volatile boolean interrupted = false;

    public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
    private static boolean FETCH_CSRF = false;

    private String jobId;
    private String jobGroup;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        jobId = context.getJobDetail().getKey().getName();
        jobGroup = context.getJobDetail().getKey().getGroup();

        JobDataMap dataMap = context.getMergedJobDataMap();
        String pixel = dataMap.getString(JobConfigKeys.PIXEL);
        String pixelParameters = dataMap.getString(JobConfigKeys.PIXEL_PARAMETERS);
        String userAccess = RPAProps.getInstance().decrypt(dataMap.getString(JobConfigKeys.USER_ACCESS));

        String execId = UUID.randomUUID().toString();
        SchedulerDatabaseUtility.insertIntoExecutionTable(execId, jobId, jobGroup);

        String keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
        String keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
        String keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);

        CloseableHttpAsyncClient asyncClient = null;
        try {
            // Create and start async client
            CookieStore httpCookieStore = new BasicCookieStore();
            asyncClient = HttpAsyncClients.custom()
                    .setDefaultCookieStore(httpCookieStore)
                    .build();
            asyncClient.start();

            boolean success = false;
            String url = Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT);
            if (url == null) {
                throw new IllegalArgumentException("Must define the scheduler endpoint to run scheduled jobs");
            }
            url = url.trim();

            Header csrfToken = null;

            if (FETCH_CSRF) {
                String fetchUrl = url.endsWith("/") ? url + "api/config/fetchCsrf" : url + "/api/config/fetchCsrf";
                SimpleHttpRequest fetchRequest = SimpleHttpRequests.get(fetchUrl);
                fetchRequest.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
                fetchRequest.setHeader("X-CSRF-Token", "fetch");

                Future<SimpleHttpResponse> fetchFuture = asyncClient.execute(fetchRequest, null);

                try {
                    SimpleHttpResponse fetchResponse = fetchFuture.get(10, TimeUnit.SECONDS);
                    csrfToken = fetchResponse.getHeader("X-CSRF-Token");
                } catch (Exception e) {
                    logger.error(Constants.STACKTRACE, e);
                }
            }

            String postUrl = url.endsWith("/") ? url + "api/schedule/executePixel" : url + "/api/schedule/executePixel";
            SimpleHttpRequest postRequest = SimpleHttpRequests.post(postUrl);
            postRequest.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
            if (csrfToken != null) {
                postRequest.setHeader("X-CSRF-Token", csrfToken);
            }

            // Prepare form parameters
            List<NameValuePair> paramList = new ArrayList<>();
            paramList.add(new BasicNameValuePair(JobConfigKeys.EXEC_ID, execId));
            paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_ID, jobId));
            paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_GROUP, jobGroup));
            paramList.add(new BasicNameValuePair(JobConfigKeys.USER_ACCESS, userAccess));

            boolean hasParam = false;
            if (pixelParameters != null && !(pixelParameters = pixelParameters.trim()).isEmpty()) {
                if (pixelParameters.endsWith(";")) {
                    pixelParameters = pixelParameters.substring(0, pixelParameters.length() - 1);
                }
                if (!pixelParameters.isEmpty()) {
                    hasParam = true;
                    paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixelParameters + " | " + pixel));
                }
            }
            if (!hasParam) {
                paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixel));
            }

            StringBuilder formBody = new StringBuilder();
            for (int i = 0; i < paramList.size(); i++) {
                NameValuePair pair = paramList.get(i);
                formBody.append(pair.getName()).append("=").append(pair.getValue());
                if (i < paramList.size() - 1) {
                    formBody.append("&");
                }
            }
            postRequest.setBody(
                formBody.toString().getBytes("UTF-8"),
                ContentType.APPLICATION_FORM_URLENCODED
            );

            long start = System.currentTimeMillis();

            Future<SimpleHttpResponse> postFuture = asyncClient.execute(postRequest, null);

            String schedulerOutput = null;
            int status = -1;

            while (!postFuture.isDone()) { //INTERUPT
                if (interrupted) {
                    postFuture.cancel(true);
                    long end = System.currentTimeMillis();
                    logger.info("##SCHEDULED JOB: " + jobId + " interrupted, exiting early after " + (end - start) / 1000 + " seconds.");
                    SchedulerDatabaseUtility.insertIntoAuditTrailTable(
                        jobId, jobGroup, start, System.currentTimeMillis(), false, "Job interrupted"
                    );
                    throw new JobExecutionException("Job interrupted and HTTP request cancelled.");
                }
            }

            try {
                SimpleHttpResponse postResponse = postFuture.get();
                status = postResponse.getCode();
                schedulerOutput = postResponse.getBodyText();
                if (status == 200) {
                    success = true;
                }
            } catch (Exception e) {
                logger.error(Constants.STACKTRACE, e);
            }

            logger.info("##SCHEDULED JOB: Response Code " + status);

            long end = System.currentTimeMillis();
            SchedulerDatabaseUtility.insertIntoAuditTrailTable(jobId, jobGroup, start, end, success, schedulerOutput);
            logger.info("##SCHEDULED JOB: Execution time: " + (end - start) / 1000 + " seconds.");

        } catch (Exception e) {
            logger.error(Constants.STACKTRACE, e);
        } finally {
            SchedulerDatabaseUtility.removeExecutionId(execId);
            if (asyncClient != null) {
                try {
                    asyncClient.close();
                } catch (IOException e) {
                    logger.error(Constants.STACKTRACE, e);
                }
            }
        }
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("Interrupt requested for job " + jobId);
        interrupted = true;
//        if (executingThread != null) {
//            executingThread.interrupt(); // Propagate interrupt to the running thread
//        }
    }

    public static void setFetchCsrf(boolean fetchCsrf) {
        RunPixelJobFromDB.FETCH_CSRF = fetchCsrf;
    }
}
