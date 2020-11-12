package prerna.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class ChromeDriverUtility {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private static String contextPath = null;
	private static String sessionCookie = null;
	private static ChromeDriver driver = null;

	public static void captureImage(String feUrl, String url, String imagePath, String sessionId) {
		captureImage(feUrl, url, imagePath, sessionId, 1920, 1080, true);
	}
	
	public static ChromeDriver makeChromeDriver(String feUrl, String url,String sessionId, int height, int width)
	{
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String os = System.getProperty("os.name").toUpperCase();
		String sysProp = baseFolder + DIR_SEPARATOR + "config" + DIR_SEPARATOR + "Chromedriver" + DIR_SEPARATOR;
		
		boolean linux = false;
		if(os.contains("WIN")){
			sysProp += "chromedriver-win.exe";
		} else if(os.contains("MAC")) {
			sysProp += "chromedriver-mac";
		} else {
			linux = true;
			sysProp += "chromedriver-linux";
		}
		System.setProperty("webdriver.chrome.driver", sysProp);

	    ChromeOptions chromeOptions = new ChromeOptions();
	    String customGoogleBinaryLocation = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_BINARY);
	    if(customGoogleBinaryLocation != null && !customGoogleBinaryLocation.isEmpty()) {
	    	chromeOptions.setBinary(customGoogleBinaryLocation);
	    }
		chromeOptions.addArguments("--headless");
		chromeOptions.addArguments("--disable-gpu");
		chromeOptions.addArguments("--window-size=" + height + "," + width);
		chromeOptions.addArguments("--remote-debugging-port=9222");
		if(linux) {
			chromeOptions.addArguments("-disable-dev-shm-usage");
			chromeOptions.addArguments("--no-sandbox");
		}
		if(url.contains("localhost") && url.contains("https")) {
			chromeOptions.addArguments("--allow-insecure-localhost ");
		}
		ChromeDriver newdriver = new ChromeDriver(chromeOptions);

		return newdriver;
		
	}

	public static void captureImagePersistent(ChromeDriver driver, String feUrl, String url, String imagePath, String sessionId) 
	{
		// need to go to the base url first
		// so that the cookie is applied at root level
		if(ChromeDriverUtility.contextPath != null) {
			String startingUrl = feUrl;
			if(startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length()-1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/")+1) + ChromeDriverUtility.contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}
		if(sessionId != null && ChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration, secure, http only
//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, null, "/", null, secure, true);
			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
			driver.manage().addCookie(name);
		}
		
		driver.navigate().to(url);
		
		// time for FE to render the page before the image is taken
	    try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// take image
		File scrFile = (File)((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
		try { 
			FileUtils.copyFile(scrFile, new File(imagePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void captureDataPersistent(ChromeDriver driver, String feUrl, String url, String sessionId) 
	{
		// need to go to the base url first
		// so that the cookie is applied at root level
		if(ChromeDriverUtility.contextPath != null) {
			String startingUrl = feUrl;
			if(startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length()-1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/")+1) + ChromeDriverUtility.contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}
		if(sessionId != null && ChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration, secure, http only
//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, null, "/", null, secure, true);
			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
			driver.manage().addCookie(name);
		}
		
		driver.navigate().to(url);
		
		// time for FE to render the page before the image is taken
	    try {
			Thread.sleep(8000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Capture the image of from a url
	 * @param feUrl the base semoss url
	 * @param url the insight embed url
	 * @param imagePath location to save image
	 * @param sessionId user session id if logged in
	 */
	public static ChromeDriver captureImage(String feUrl, String url, String imagePath, String sessionId, int height, int width,  boolean close) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		// load driver options
		String os = System.getProperty("os.name").toUpperCase();
		String sysProp = baseFolder + DIR_SEPARATOR + "config" + DIR_SEPARATOR + "Chromedriver" + DIR_SEPARATOR;
		boolean linux = false;
		if(os.contains("WIN")){
			sysProp += "chromedriver-win.exe";
		} else if(os.contains("MAC")) {
			sysProp += "chromedriver-mac";
		} else {
			linux = true;
			sysProp += "chromedriver-linux";
		}
		System.setProperty("webdriver.chrome.driver", sysProp);
		boolean secure = url.contains("https");
		
	    ChromeOptions chromeOptions = new ChromeOptions();
	    String customGoogleBinaryLocation = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_BINARY);
	    if(customGoogleBinaryLocation != null && !customGoogleBinaryLocation.isEmpty()) {
	    	chromeOptions.setBinary(customGoogleBinaryLocation);
	    }
		chromeOptions.addArguments("--headless");
		chromeOptions.addArguments("--disable-gpu");
		chromeOptions.addArguments("--window-size=" + height + "," + width);
		chromeOptions.addArguments("--remote-debugging-port=9222");
		if(linux) {
			chromeOptions.addArguments("-disable-dev-shm-usage");
			chromeOptions.addArguments("--no-sandbox");
		}
		if(url.contains("localhost") && url.contains("https")) {
			chromeOptions.addArguments("--allow-insecure-localhost ");
		}
		driver = new ChromeDriver(chromeOptions);
	
		// need to go to the base url first
		// so that the cookie is applied at root level
		if(ChromeDriverUtility.contextPath != null) {
			String startingUrl = feUrl;
			if(startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length()-1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/")+1) + ChromeDriverUtility.contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}
		if(sessionId != null && ChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration, secure, http only
//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, null, "/", null, secure, true);
			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
			driver.manage().addCookie(name);
		}
		driver.navigate().to(url);
		
		// time for FE to render the page before the image is taken
	    try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// take image
		File scrFile = (File)((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
		try { 
			FileUtils.copyFile(scrFile, new File(imagePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(close) {
			driver.quit();
		}
		return driver;
	}
	
	
	public static void setContextPath(String contextPath) {
		if(contextPath.startsWith("/")) {
			contextPath = contextPath.substring(1);
		}
		if(contextPath.endsWith("/")) {
			contextPath = contextPath.substring(0, contextPath.length()-1);
		}
		ChromeDriverUtility.contextPath = contextPath;
	}
	
	public static void setSessionCookie(String sessionCookie) {
		ChromeDriverUtility.sessionCookie = sessionCookie;
	}
}
