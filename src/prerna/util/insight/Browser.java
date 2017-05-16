package prerna.util.insight;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.*;


import javafx.geometry.HPos;
import javafx.geometry.VPos;
import javafx.scene.layout.Region;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;

@SuppressWarnings("restriction")
class Browser extends Region {

	private final WebView browser = new WebView();

	Browser(String url) {
		// Create a trust manager that does not validate certificate chains
//		TrustManager[] trustAllCerts = new TrustManager[] { 
//		    new TrustManager() {     
//		        public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
//		            return null;
//		        } 
//		        public void checkClientTrusted( 
//		            java.security.cert.X509Certificate[] certs, String authType) {
//		            } 
//		        public void checkServerTrusted( 
//		            java.security.cert.X509Certificate[] certs, String authType) {
//		        }
//		    } 
//		}; 
//
//		// Install the all-trusting trust manager
//		try {
//		    SSLContext sc = SSLContext.getInstance("SSL"); 
//		    sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
//		    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
//		} catch (GeneralSecurityException e) {
//		} 
		
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { 
		    new X509TrustManager() {     
		        public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
		            return null;
		        } 
		        public void checkClientTrusted( 
		            java.security.cert.X509Certificate[] certs, String authType) {
		            } 
		        public void checkServerTrusted( 
		            java.security.cert.X509Certificate[] certs, String authType) {
		        }
		    } 
		}; 

		// Install the all-trusting trust manager
		try {
		    SSLContext sc = SSLContext.getInstance("SSL"); 
		    sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
		    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		} catch (GeneralSecurityException e) {
		}
		// Now you can access an https URL without having the certificate in the truststore
		try { 
		    URL url2 = new URL(url); 
			WebEngine webEngine = browser.getEngine();
			webEngine.load(url2.toString());
			
			   webEngine.getLoadWorker().stateProperty()
	            .addListener((ov, oldState, newState) -> {
	                System.err.println(webEngine.getLoadWorker()
	                         .exceptionProperty());
	            });
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} 
		

		

		getChildren().add(browser);

	}

	Boolean isPageLoaded() {
		try {
		if (((Boolean) browser.getEngine().executeScript("window.visualLoaded;")) == true) {
			return true;
		}
		}
		catch (ClassCastException cce) {
			
		}
		
		return false;
	}

	@Override
	protected void layoutChildren() {
		double w = getWidth();
		double h = getHeight();
		layoutInArea(browser, 0, 0, w, h, 0, HPos.CENTER, VPos.CENTER);
	}

	@Override
	protected double computePrefWidth(double width) {
		return 1280;
	}

	@Override
	protected double computePrefHeight(double height) {
		return 720;
	}

}
