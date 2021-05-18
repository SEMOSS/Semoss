package prerna.rpa.quartz.jobs.insight;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.net.ssl.HostnameVerifier;

import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.RPAProps;
import prerna.rpa.config.JobConfigKeys;
import prerna.rpa.quartz.CommonDataKeys;
import prerna.sablecc2.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RunPixelJobFromDB implements InterruptableJob {

	private static final Logger logger = LogManager.getLogger(RunPixelJobFromDB.class);

	public static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	public static final String OUT_INSIGHT_ID_KEY = CommonDataKeys.INSIGHT_ID;

	private String jobId;
	private String jobGroup;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobId = context.getJobDetail().getKey().getName();
		jobGroup = context.getJobDetail().getKey().getGroup();
		String pixel = dataMap.getString(JobConfigKeys.PIXEL);
		String pixelParameters = dataMap.getString(JobConfigKeys.PIXEL_PARAMETERS);
		String userAccess = RPAProps.getInstance().decrypt(dataMap.getString(JobConfigKeys.USER_ACCESS));

		String execId = UUID.randomUUID().toString();
		// insert the exec id so we allow the execution
		SchedulerDatabaseUtility.insertIntoExecutionTable(execId, jobId, jobGroup);
		
		try {
			// run the pixel endpoint
			boolean success;
			String url = DIHelper.getInstance().getProperty(Constants.SCHEDULER_ENDPOINT);
			if(url == null) {
				throw new IllegalArgumentException("Must define the scheduler endpoint to run scheduled jobs");
			}
			url = url.trim();
			if(url.endsWith("/")) {
				url += "api/schedule/executePixel";
			} else {
				url += "/api/schedule/executePixel";
			}
			
//			CloseableHttpClient httpclient = HttpClients.createDefault();
			CloseableHttpClient httpclient = getCustomClient();
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Content-Type","application/x-www-form-urlencoded; charset=utf-8");
			// add the body
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			paramList.add(new BasicNameValuePair(JobConfigKeys.EXEC_ID, execId));
			paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_ID, jobId));
			paramList.add(new BasicNameValuePair(JobConfigKeys.JOB_GROUP, jobGroup));
			paramList.add(new BasicNameValuePair(JobConfigKeys.USER_ACCESS, userAccess));
			boolean hasParam = false;
			if(pixelParameters != null && !(pixelParameters = pixelParameters.trim()).isEmpty()) {
				if(pixelParameters.endsWith(";")) {
					pixelParameters = pixelParameters.substring(0, pixelParameters.length()-1);
				}
				// account for just a ";" being sent as the pixel parameter
				if(!pixelParameters.isEmpty()) {
					hasParam = true;
					paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixelParameters + " | " + pixel));
				}
			}
			if(!hasParam) {
				paramList.add(new BasicNameValuePair(JobConfigKeys.PIXEL, pixel));
			}
			
			long start = System.currentTimeMillis();
			
			CloseableHttpResponse response = null;
			try {
				httppost.setEntity(new UrlEncodedFormEntity(paramList));
				response = httpclient.execute(httppost);
			} catch (ClientProtocolException e) {
				logger.error(Constants.STACKTRACE, e);
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			
			int status = response.getStatusLine().getStatusCode();
			
			if (status == 200 ) {
				success = true;
			} else {
				success = false;
			}
			logger.info("##SCHEDULED JOB: Response Code " + status);
//			try {
//				logger.info("##SCHEDULED JOB: Json return = " + EntityUtils.toString(response.getEntity()));
//			} catch (ParseException e) {
//				logger.error(Constants.STACKTRACE, e);
//			} catch (IOException e) {
//				logger.error(Constants.STACKTRACE, e);
//			}
			
			// store execution time and date in SMSS_AUDIT_TRAIL table
			long end = System.currentTimeMillis();
			SchedulerDatabaseUtility.insertIntoAuditTrailTable(jobId, jobGroup, start, end, success);
			logger.info("##SCHEDULED JOB: Execution time: " + (end - start) / 1000 + " seconds.");
		} finally {
			// always delete the UUID
			SchedulerDatabaseUtility.removeExecutionId(execId);
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
		logger.warn("Received request to interrupt the " + jobId
				+ " job. However, there is nothing to interrupt for this job.");
	}
	
	private CloseableHttpClient getCustomClient() {
		HttpClientBuilder builder = HttpClients.custom();
		
		TrustStrategy trustStrategy = new TrustStrategy() {
			
			@Override
			public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				return true;
			}
		};
		
		HostnameVerifier verifier = new NoopHostnameVerifier();
		SSLConnectionSocketFactory connFactory = null;
		try {
			connFactory = new SSLConnectionSocketFactory(
					SSLContextBuilder.create().loadTrustMaterial(trustStrategy).build() 
					, new String[] {"TLSv1", "TLSv1.1", "TLSv1.2", "TLSV1.3"}
					, null
					, verifier) 
//			{
//				@Override
//				protected void prepareSocket(SSLSocket socket) {
//		            socket.setEnabledProtocols(new String[] { "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3" });
//				}
//			}
			;
		} catch (KeyManagementException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (NoSuchAlgorithmException e) {
			logger.error(Constants.STACKTRACE, e);
		} catch (KeyStoreException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		builder.setSSLSocketFactory(connFactory);
		return builder.build();
	}
}
