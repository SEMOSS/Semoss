//package prerna.util.insight;
//
//import java.security.GeneralSecurityException;
//
//import javax.net.ssl.HttpsURLConnection;
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.TrustManager;
//import javax.net.ssl.*;
//
//import javafx.geometry.HPos;
//import javafx.geometry.VPos;
//import javafx.scene.layout.Region;
//import javafx.scene.web.WebEngine;
//import javafx.scene.web.WebView;
//import javafx.stage.Stage;
//import netscape.javascript.JSObject;
//
//@SuppressWarnings("restriction")
//class Browser extends Region {
//
//	private final WebView browser = new WebView();
//
//	Browser(String url, Stage window) {
//
//		// set up certificate manager for browser
//		browserSSL();
//
//		WebEngine webEngine = browser.getEngine();
//		webEngine.load(url);
//		webEngine.impl_getDebugger();
//		JSObject jsWindow = (JSObject) webEngine.executeScript("window");
//		// Log browser errors
//		webEngine.getLoadWorker().stateProperty().addListener((observable, oldValue, newValue) -> {
//			if (oldValue.name().equals("FAILED")) {
//				JSObject jsWindowError = (JSObject) webEngine.executeScript("window");
//				JavaBridge bridgeError = new JavaBridge();
//				jsWindowError.setMember("java", bridgeError);
//				webEngine.executeScript("console.log = function(message)\n" + "{\n" + "    java.log(message);\n" + "};");
//				window.close();
//			}
//		});
//
//		getChildren().add(browser);
//
//	}
//
//	private void browserSSL() {
//		// Create a trust manager that does not validate certificate chains
//		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
//			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
//				return null;
//			}
//
//			public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
//			}
//
//			public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
//			}
//		} };
//
//		// Install the all-trusting trust manager
//		try {
//			SSLContext sc = SSLContext.getInstance("SSL");
//			sc.init(null, trustAllCerts, new java.security.SecureRandom());
//			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
//		} catch (GeneralSecurityException e) {
//		}
//	}
//
//	Boolean isPageLoaded() {
//		try {
//			if (((Boolean) browser.getEngine().executeScript("window.visualLoaded;")) == true) {
//				return true;
//			}
//		} catch (ClassCastException cce) {
//
//		}
//
//		return false;
//	}
//
//	@Override
//	protected void layoutChildren() {
//		double w = getWidth();
//		double h = getHeight();
//		layoutInArea(browser, 0, 0, w, h, 0, HPos.CENTER, VPos.CENTER);
//	}
//
//	@Override
//	protected double computePrefWidth(double width) {
//		return 1280;
//	}
//
//	@Override
//	protected double computePrefHeight(double height) {
//		return 720;
//	}
//
//	class JavaBridge {
//		public void log(String text) {
//			System.out.println(text);
//		}
//	}
//
//}
