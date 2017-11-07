package prerna.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;

public class GoogleAnalytics extends Thread {

	private String thisExpression = null;
	private String prevExpression = null;
	private String thisType = null;
	private String thisprevType = null;
	private String userID = null;


	public GoogleAnalytics(String thisExpression, String thisType) {
		this(thisExpression, thisType, null, null, null);
	}
	
	public GoogleAnalytics(String thisExpression, String thisType, String prevExpression, String prevType) {
		this(thisExpression, thisType, prevExpression, prevType, null);
	}
	
	public GoogleAnalytics(String thisExpression, String thisType, String userID) {
		this(thisExpression, thisType, null, null, userID);
	}

	public GoogleAnalytics(String thisExpression, String thisType, String prevExpression, String prevType, String userID) {
		this.thisExpression = thisExpression;
		this.prevExpression = prevExpression;
		this.thisType= thisType;
		this.thisprevType= prevType;
		this.userID= userID;
	}

	@Override
	public void run() {
		String curType = thisType;
		String prevType = thisprevType;
		String eventLabel = thisExpression;
		String previousEvent = prevExpression;
		String uID = userID;
		String IP;
		//get the user's IP address to use as user ID.
		//this isnt useful for clients with semoss BE hosted on a server
		try {
			IP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			IP = "127.0.0.1";
		}
		if (previousEvent == null){
			previousEvent = "";
		}
		if (prevType == null){
			prevType = "";
		}
		//if the user signed into Semoss, then append 
		// user ID to IP address
		if (uID != null){
			IP = IP + "_" + uID;
		}
		HttpClient client = HttpClientBuilder.create().build();
		//build uri to send to GA using their measurement protocol, other 
		//data can be added as needed.
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme("http")
		.setHost("www.google-analytics.com")
		.setPath("/collect")
		.addParameter("v", "1")
		.addParameter("t", "event")
		.addParameter("tid", "UA-99971122-1")
		.addParameter("cid", IP)
		.addParameter("cd1", curType)
		.addParameter("cd2", eventLabel)
		.addParameter("cd3", prevType)
		.addParameter("cd4", previousEvent)
		.addParameter("cd5", IP)
		.addParameter("ec", "Custom Category")
		.addParameter("ea", "Custom Action")
		.addParameter("el", "Custom Label");

		java.net.URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			return;
		}
		System.out.println("GOOGLE ANALYTICS: "+uri);
		HttpPost post = new HttpPost(uri);
		try {
			client.execute(post);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
		}
	}
}
