package prerna.rpa.quartz.jobs.insight;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.cookie.BasicCookieStore;
import org.apache.hc.client5.http.cookie.CookieStore;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicNameValuePair;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import java.net.URLEncoder;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.NameValuePair;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigKeys;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunPixelJobFromDB implements InterruptableJob {

	private static final Logger classLogger = LogManager.getLogger(RunPixelJobFromDB.class);

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static boolean FETCH_CSRF = false;
	private volatile boolean interrupted = false;
	
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
		// insert the exec id so we allow the execution
		SchedulerDatabaseUtility.insertIntoExecutionTable(execId, jobId, jobGroup);
		// add the scheduler cert if required
		String keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
		String keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
		String keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		
		 CloseableHttpAsyncClient asyncClient = null;
		
		try {
			// Initialize Async HTTP Client
            CookieStore cookieStore = new BasicCookieStore();
            asyncClient = HttpAsyncClients.custom()
                    .setDefaultCookieStore(cookieStore)
                    .build();
            asyncClient.start();
			
			// run the pixel endpoint
			boolean success = false;
			String schedulerOutput = null;
			String url = Utility.getDIHelperProperty(Constants.SCHEDULER_ENDPOINT);
			if(url == null) {
				throw new IllegalArgumentException("Must define the scheduler endpoint to run scheduled jobs");
			}
			url = url.trim();
			long startTime = System.currentTimeMillis();
			
			Header csrfToken = null;
			if (FETCH_CSRF) {
                csrfToken = fetchCsrfToken(asyncClient, url);
            }

            //Build HTTP Request
            String postUrl = url.endsWith("/") ? url + "api/schedule/executePixel" : url + "/api/schedule/executePixel";
            SimpleHttpRequest postRequest = SimpleRequestBuilder.post(postUrl).build();
            postRequest.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
            if (csrfToken != null) {
                postRequest.setHeader("X-CSRF-Token", csrfToken);
            }

            String body = buildRequestBody(execId, jobId, jobGroup, userAccess, pixel, pixelParameters);
            postRequest.setBody(body.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_FORM_URLENCODED);

            
            //Execute Request Asynchronously
            Future<SimpleHttpResponse> future = asyncClient.execute(postRequest, null);

            //Monitor Interruption
            while (!future.isDone()) {
                if (interrupted) {
                	classLogger.warn("Interrupt detected for job {}. Cancelling async HTTP request...", jobId);
                    future.cancel(true);
                    recordAuditTrail(jobId, jobGroup, startTime, false, "Job interrupted before completion");
                    throw new JobExecutionException("Job interrupted and HTTP request cancelled.");
                }
                Thread.sleep(500);
            }
            
			
            //Handle Response
            SimpleHttpResponse response = future.get();
            int statusCode = response.getCode();
            schedulerOutput = response.getBodyText();

            success = (statusCode == 200);
            recordAuditTrail(jobId, jobGroup, startTime, success, schedulerOutput);

            classLogger.info("##SCHEDULED JOB: {} completed with status {}, duration={} sec",
                    jobId, statusCode, (System.currentTimeMillis() - startTime) / 1000);
//			try {
//				logger.info("##SCHEDULED JOB: Json return = " + EntityUtils.toString(response.getEntity()));
//			} catch (ParseException e) {
//				logger.error(Constants.STACKTRACE, e);
//			} catch (IOException e) {
//				logger.error(Constants.STACKTRACE, e);
//			}
			
		} catch (JobExecutionException e) {
            throw e; 
        } catch (Exception e) {
        	classLogger.error("Unexpected error while executing async job {}", jobId, e);
        } finally {
            SchedulerDatabaseUtility.removeExecutionId(execId);
            closeClient(asyncClient);
        }
		
//		// Execute job
//		Insight insight = new Insight();
//
//		// Add user info to the insight
//		User user = new User();
//		String[] accessPairs = userAccess.split(",");
//
//		for (String accessPair : accessPairs) {
//			String[] providerAndId = accessPair.split(":");
//
//			// Get the auth provider
//			AuthProvider provider = AuthProvider.valueOf(providerAndId[0]);
//
//			// Get the id
//			String id = providerAndId[1];
//
//			// Create the access token
//			AccessToken token = new AccessToken();
//			token.setProvider(provider);
//			token.setId(id);
//
//			user.setAccessToken(token);
//		}
//		insight.setUser(user);
//
//		String insightId = InsightStore.getInstance().put(insight);
//		if (!pixel.endsWith(";")) {
//			pixel = pixel + ";";
//		}
//
//		// make a random session id
//		ThreadStore.setInsightId(insightId);
//		ThreadStore.setSessionId("scheduledJob_" + UUID.randomUUID().toString());
//		ThreadStore.setJobId(insightId);
//		ThreadStore.setUser(user);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		 classLogger.warn("Interrupt request received for job: " + jobId);
		 interrupted = true;
	}
	
	public static void setFetchCsrf(boolean fetchCsrf) {
		RunPixelJobFromDB.FETCH_CSRF = fetchCsrf;
	}
	
	private Header fetchCsrfToken(CloseableHttpAsyncClient client, String url) {
        String fetchUrl = url.endsWith("/") ? url + "api/config/fetchCsrf" : url + "/api/config/fetchCsrf";
        SimpleHttpRequest fetchRequest = SimpleRequestBuilder.get(fetchUrl).build();
        fetchRequest.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        fetchRequest.setHeader("X-CSRF-Token", "fetch");

        try {
            Future<SimpleHttpResponse> future = client.execute(fetchRequest, null);
            SimpleHttpResponse response = future.get(5, TimeUnit.SECONDS);
            return response.getHeader("X-CSRF-Token");
        } catch (Exception e) {
        	classLogger.error("Failed to fetch CSRF token: {}", e.getMessage());
            return null;
        }
    }
	
	private String buildRequestBody(String execId, String jobId, String jobGroup, String userAccess, String pixel,
			String pixelParameters) {
		List<NameValuePair> params = new ArrayList<>();
		params.add(new BasicNameValuePair(JobConfigKeys.EXEC_ID, execId));
		params.add(new BasicNameValuePair(JobConfigKeys.JOB_ID, jobId));
		params.add(new BasicNameValuePair(JobConfigKeys.JOB_GROUP, jobGroup));
		params.add(new BasicNameValuePair(JobConfigKeys.USER_ACCESS, userAccess));

		if (pixelParameters != null && !(pixelParameters = pixelParameters.trim()).isEmpty()) {
			if (pixelParameters.endsWith(";")) {
				pixelParameters = pixelParameters.substring(0, pixelParameters.length() - 1);
			}
			if (!pixelParameters.isEmpty()) {
				params.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixelParameters + " | " + pixel));
			}
		} else {
			params.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixel));
		}

		return params.stream().map(p -> p.getName() + "=" + URLEncoder.encode(p.getValue(), StandardCharsets.UTF_8))
				.collect(Collectors.joining("&"));
	}
	
	private void recordAuditTrail(String jobId, String jobGroup, long start, boolean success, String output) {
        SchedulerDatabaseUtility.insertIntoAuditTrailTable(jobId, jobGroup, start, System.currentTimeMillis(), success, output);
    }

    private void closeClient(CloseableHttpAsyncClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
            	classLogger.error("Error closing async HTTP client", e);
            }
        }
    }

}
