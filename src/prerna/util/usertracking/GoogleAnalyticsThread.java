//package prerna.util.usertracking;
//
//import java.io.IOException;
//import java.net.URISyntaxException;
//
//import org.apache.http.client.ClientProtocolException;
//import org.apache.http.client.HttpClient;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.client.utils.URIBuilder;
//import org.apache.http.impl.client.HttpClientBuilder;
//
//public class GoogleAnalyticsThread extends Thread {
//
//	private String thisExpression = null;
//	private String prevExpression = null;
//	private String thisType = null;
//	private String thisprevType = null;
//	private String userId = null;
//
//	/*
//	 * All constructors are protected so only GoogleAnalytics class can actually use it
//	 */
//	
//	protected GoogleAnalyticsThread(String thisExpression, String thisType) {
//		this(thisExpression, thisType, null, null, null);
//	}
//
//	protected GoogleAnalyticsThread(String thisExpression, String thisType, String prevExpression, String prevType) {
//		this(thisExpression, thisType, prevExpression, prevType, null);
//	}
//
//	protected GoogleAnalyticsThread(String thisExpression, String thisType, String prevExpression, String prevType, String userId) {
//		this.thisExpression = thisExpression;
//		this.prevExpression = prevExpression;
//		this.thisType= thisType;
//		this.thisprevType= prevType;
//		this.userId = userId;
//	}
//
//	@Override
//	public void run() {
//		String curType = thisType;
//		String prevType = thisprevType;
//		String eventLabel = thisExpression;
//		String previousEvent = prevExpression;
//		String ID = System.getProperty("user.name");
//
//		if (previousEvent == null){
//			previousEvent = "";
//		}
//		if (prevType == null){
//			prevType = "";
//		}
//		HttpClient client = HttpClientBuilder.create().build();
//		//build uri to send to GA using their measurement protocol
//		URIBuilder builder = new URIBuilder();
//		builder
//		.setScheme("http")
//		.setHost("www.google-analytics.com")
//		.setPath("/collect")
//		.addParameter("v", "1")
//		.addParameter("t", "event")
//		.addParameter("tid", "UA-99971122-1")
//		.addParameter("cid", ID)
//		.addParameter("cd1", curType)
//		.addParameter("cd2", eventLabel)
//		.addParameter("cd3", prevType)
//		.addParameter("cd4", previousEvent)
//		.addParameter("cd5", ID)
//		.addParameter("ec", "Custom Category")
//		.addParameter("ea", "Custom Action")
//		.addParameter("el", "Custom Label");
//
//		java.net.URI uri = null;
//		try {
//			uri = builder.build();
//		} catch (URISyntaxException e) {
//			return;
//		}
//
//		HttpPost post = new HttpPost(uri);
//		try {
//			client.execute(post);
//		} catch (ClientProtocolException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		} catch (IOException e) {
//		}
//	}
//	
//}
