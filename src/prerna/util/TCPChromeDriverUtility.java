package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import prerna.om.ThreadStore;
import prerna.test.TestUtilityMethods;
import prerna.util.insight.InsightUtility;

public class TCPChromeDriverUtility {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	protected static final Logger logger = LogManager.getLogger(InsightUtility.class.getName());

	protected static String contextPath = null;
	protected static String sessionCookie = null;

	public static ChromeDriver driver = null;

	public static void captureImage(String feUrl, String url, String imagePath, String sessionId) {
		ChromeDriver thisDriver = null;
		try {
			thisDriver = (ChromeDriver)makeChromeDriver(feUrl, url, 1920, 1080);
			captureImagePersistent(thisDriver, feUrl, url, imagePath, sessionId, 800);
		} finally {
			if(thisDriver != null) {
				thisDriver.close();
			}
		}
	}

	public static Object makeChromeDriver(String feUrl, String url, int height, int width) 
	{
		if(TCPChromeDriverUtility.driver == null)
		{
			String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String os = System.getProperty("os.name").toUpperCase();
			String sysProp = baseFolder + DIR_SEPARATOR + "config" + DIR_SEPARATOR + "Chromedriver" + DIR_SEPARATOR;

			boolean linux = false;
			if (os.contains("WIN")) {
				sysProp += "chromedriver-win.exe";
			} else if (os.contains("MAC")) {
				sysProp += "chromedriver-mac";
			} else {
				linux = true;
				sysProp += "chromedriver-linux";
			}
			System.setProperty("webdriver.chrome.driver", sysProp);
			// System.setProperty("webdriver.chrome.verboseLogging", "true");
			System.setProperty("webdriver.chrome.whitelistedIps", "");

			ChromeOptions chromeOptions = new ChromeOptions();
			String customGoogleBinaryLocation = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_BINARY);
			if (customGoogleBinaryLocation != null && !customGoogleBinaryLocation.isEmpty()) {
				chromeOptions.setBinary(customGoogleBinaryLocation);
			}
			chromeOptions.addArguments("--headless");
			chromeOptions.addArguments("--disable-gpu");
			chromeOptions.addArguments("--window-size=" + height + "," + width);
			chromeOptions.addArguments("--remote-debugging-port=9222");
			//logger.info("##CHROME DRIVER: allowing insecure local");
			//logger.info("##CHROME DRIVER: ignore certs");

			//chromeOptions.addArguments("--allow-insecure-localhost");
			chromeOptions.addArguments("--ignore-certificate-errors");
			chromeOptions.addArguments("--ignore-ssl-errors");
			chromeOptions.addArguments("--ignore-ssl-errors=yes");
			chromeOptions.addArguments("--ignore-ssl-errors=true");

			if (linux) {
				chromeOptions.addArguments("-disable-dev-shm-usage");
				chromeOptions.addArguments("--no-sandbox");
			}
			if (url.contains("localhost") && url.contains("https")) {
				chromeOptions.addArguments("--allow-insecure-localhost ");
			}
			TCPChromeDriverUtility.driver = new ChromeDriver(chromeOptions);
		}
		return new Object();
	}

	public static String captureImagePersistent(Object driverObj, String feUrl, String url, String imagePath, String sessionId, int waitTime) {
		// need to go to the base url first
		// so that the cookie is applied at root level
		driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);

		if (TCPChromeDriverUtility.contextPath != null) {
			logger.info("##CHROME DRIVER: starting url = "+ url);

			logger.info("##CHROME DRIVER: context path not null = "+ TCPChromeDriverUtility.contextPath);
			logger.info("##CHROME DRIVER: starting feUrl = "+ feUrl);

			String startingUrl = feUrl;
			if (startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length() - 1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/") + 1)
					+ TCPChromeDriverUtility.contextPath;

			logger.info("##CHROME DRIVER: ending baseUrl = "+ baseUrl);
			//logger.info("##CHROME DRIVER: don't care using feURL " + feUrl);

			driver.get(baseUrl);
		} else {
			driver.get(url);
			logger.info("##CHROME DRIVER: contextPath is null");
			logger.info("##CHROME DRIVER: url to get = "+ url);

		}

		if (sessionId != null && TCPChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration
			//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, feUrl, "/", null);
			updateCookie(driver, TCPChromeDriverUtility.sessionCookie, sessionId);
			String route = ThreadStore.getRouteId();
			if(route == null || route.isEmpty()) {
				route = ChromeDriverUtility.routeCookieValue;
			}
			if(route != null && !route.isEmpty()) {
				String routeCookieName = DIHelper.getInstance().getProperty(Constants.MONOLITH_ROUTE);
				if (routeCookieName != null && !routeCookieName.isEmpty()) {
					updateCookie(driver, routeCookieName, route);
				}

			} else {
				logger.info("##CHROME DRIVER: routeID in threadstore is null or empty");
			}
			//Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
			//driver.manage().addCookie(name);
		}

		// url = url + "&status";
		driver.navigate().to(url);

		// looking for viz loaded
		/*
		 * WebElement we = null; we =
		 * driver.findElement(By.xpath("//html/body//div[@id='viz-loaded']")); //we =
		 * new WebDriverWait(driver,
		 * 10).until(ExpectedConditions.elementToBeClickable(By.xpath(
		 * "//html/body//div[@id='viz-loaded']")));
		 * 
		 * String html2 = driver.executeScript("return arguments[0].outerHTML;", we) +
		 * ""; //logger.info(html2);
		 */

		// time for FE to render the page before the image is taken
		try {
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// take image
		File srcFile = (File) ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
		try {
			//FileUtils.copyFile(srcFile, new File(imagePath));
			//FileUtils.moveFile(srcFile, new File(imagePath));
			File targetFile = new File(imagePath);
			//logger.debug("source file has been written to.. " + srcFile.getAbsolutePath());
			copyFile(srcFile, targetFile);
			while(!targetFile.exists()) // wait for the file to be written
			{
				Thread.sleep(50);
			}
			return srcFile.getAbsolutePath();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private static void copyFile(File srcFile, File targetFile) throws Exception
	{

		FileInputStream input=null;
		FileOutputStream output = null;
		try {
			input = new FileInputStream(srcFile);
			output = new FileOutputStream(targetFile);

			byte[] buf = new byte[1024];
			int bytesRead;

			while ((bytesRead = input.read(buf)) > 0) {
				output.write(buf, 0, bytesRead);
			}
		} finally {

			if(input!=null) {
				input.close();
			}

			if(output!=null) {
				output.close();
			}

		}
	}

	protected static void updateCookie(ChromeDriver driver, String cookieName, String cookieValue) {
		logger.info("##CHROME DRIVER: driver is looking at " + driver.getCurrentUrl());
		logger.info("##CHROME DRIVER: driver is looking page source at " + driver.getPageSource());

		logger.info("##CHROME DRIVER: looking cookie with Name = "+ cookieName);

		Iterator<Cookie> cooki2 = driver.manage().getCookies().iterator();
		while (cooki2.hasNext()) {
			Cookie cook3 = (Cookie) cooki2.next();
			String name2 = cook3.getName();
			logger.info("##CHROME DRIVER: INIT CHECK found cookie" + cook3.toJson());
		}

		Iterator<Cookie> cooki = driver.manage().getCookies().iterator();
		boolean cookieFound = false;
		Cookie cook = null;
		// remove if such a cookie exists
		while (cooki.hasNext())
		{
			cook = (Cookie) cooki.next();
			String name = cook.getName();
			if (name.equalsIgnoreCase(cookieName)) {
				logger.info("##CHROME DRIVER: found cookie with Name = "+ cookieName);

				//driver.manage().deleteCookie(cook);

				//logger.info("##CHROME DRIVER: deleted cookie with Name = "+ cookieName);
				cookieFound = true;
				break;
			}
		}

		if(cookieFound) {
			logger.info("##CHROME DRIVER: found cookie - Name " + cook.getName() 
			+ " domain: " + cook.getDomain() 
			+ " path: " +  cook.getPath()
			+ " isHttpOnly: " +  cook.isHttpOnly()
			+ " isSecure: " +  cook.isSecure()	
			+ " value: " + cook.getValue()
					);
			driver.manage().deleteCookie(cook);
			logger.info("##CHROME DRIVER: deleted cookie with Name = "+ cookieName);
			Cookie name= new Cookie(cook.getName(),
					cookieValue,
					cook.getDomain(),
					cook.getPath(),
					cook.getExpiry(),
					cook.isSecure(),
					cook.isHttpOnly());
			logger.info("##CHROME DRIVER: Adding cookie  - name: " + name.getName()
			+ " domain: " + name.getDomain() 
			+ " path: " +  name.getPath()
			+ " isHttpOnly: " +  name.isHttpOnly()
			+ " isSecure: " +  name.isSecure()	
			+ " value: " + name.getValue()
					);
			// works - but doesnt login
			driver.manage().addCookie(name);
		} else { 
			logger.info("##CHROME DRIVER: cookie not found " + cookieName);

			//Date expiresDate = new Date(new Date().getTime() + 36000*1000); 

			//Cookie name = new Cookie(cookieName, cookieValue, "/", expiresDate); // , null);
			Cookie name = new Cookie(cookieName, cookieValue, "/"); // , null);

			//			logger.info("##CHROME DRIVER: MODDED COOKIE");
			//
			//			Cookie name= new Cookie(cookieName,
			//					cookieValue,
			//					"semosscontainer-healthx-dev.apps.ent-ocp-np1-har.antmdc.internal.das",
			//					"/Monolith",
			//					null,
			//					true,
			//					true);
			logger.info("##CHROME DRIVER: BASE ADD Adding cookie  - name: " + name.getName()
			+ " domain: " + name.getDomain() 
			+ " path: " +  name.getPath()
			+ " isHttpOnly: " +  name.isHttpOnly()
			+ " isSecure: " +  name.isSecure()	
			+ " value: " + name.getValue()
			+ " age: " + name.getExpiry()
			+ " json: " + name.toJson()
					);
			// works - but doesnt login
			driver.manage().addCookie(name);
		}
	}

	public static String captureDataPersistent(Object driverObj, String feUrl, String url, String sessionId, int waitTime) {
		// need to go to the base url first
		// so that the cookie is applied at root level
		// driver.manage().timeouts().implicitlyWait(10,TimeUnit.SECONDS) ;


		if (TCPChromeDriverUtility.contextPath != null) {
			logger.info("##CHROME DRIVER: starting url = "+ url);

			logger.info("##CHROME DRIVER: context path not null = "+ TCPChromeDriverUtility.contextPath);
			logger.info("##CHROME DRIVER: starting feUrl = "+ feUrl);

			String startingUrl = feUrl;
			if (startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length() - 1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/") + 1)
					+ TCPChromeDriverUtility.contextPath;

			logger.info("##CHROME DRIVER: ending baseUrl = "+ baseUrl);

			TCPChromeDriverUtility.driver.get(baseUrl);
		} else {
			TCPChromeDriverUtility.driver.get(url);
			logger.info("##CHROME DRIVER: contextPath is null");
			logger.info("##CHROME DRIVER: url to get = "+ url);
		}
		TCPChromeDriverUtility.driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);

		if (sessionId != null && TCPChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration
			//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, feUrl, "/", null);
			updateCookie(driver, TCPChromeDriverUtility.sessionCookie, sessionId);
			String route = ThreadStore.getRouteId();
			if(route == null || route.isEmpty()) {
				route = ChromeDriverUtility.routeCookieValue;
			}
			if(route != null && !route.isEmpty()) {
				String routeCookieName = DIHelper.getInstance().getProperty(Constants.MONOLITH_ROUTE);
				if (routeCookieName != null && !routeCookieName.isEmpty()) {
					updateCookie(TCPChromeDriverUtility.driver, routeCookieName, route);
				}
			} else {
				logger.info("##CHROME DRIVER: routeID in threadstore is null or empty");
			}
			//Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
			//driver.manage().addCookie(name);
		}

		logger.info("Chrome -- Navingating to URL  " + url);
		TCPChromeDriverUtility.driver.navigate().to(url);

		// add a sleep
		try {
			Thread.sleep(waitTime);
		} catch (InterruptedException e) {
			e.printStackTrace(); 
		}

		// trying the wait
		WebDriverWait wait = new WebDriverWait(TCPChromeDriverUtility.driver, 600); // giving it 10 min.. we need a better way for this but.. 
		WebElement we1 = wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//html/body//table")));

		logger.info(" The element output I got is " + we1.getText());

		//String html2 = getHTML(TCPChromeDriverUtility.driver, "//html/body//table");
		return we1.getText();

	}

	public static void setContextAndSessionCookie(String contextPath, String sessionCookie)
	{
		TCPChromeDriverUtility.sessionCookie = sessionCookie;
		TCPChromeDriverUtility.contextPath = contextPath;
	}


	//	/**
	//	 * Capture the image of from a url
	//	 * 
	//	 * @param feUrl     the base semoss url
	//	 * @param url       the insight embed url
	//	 * @param imagePath location to save image
	//	 * @param sessionId user session id if logged in
	//	 */
	//	public static ChromeDriver captureImage(String feUrl, String url, String imagePath, String sessionId, int height,
	//			int width, boolean close) {
	//		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	//		// load driver options
	//		String os = System.getProperty("os.name").toUpperCase();
	//		String sysProp = baseFolder + DIR_SEPARATOR + "config" + DIR_SEPARATOR + "Chromedriver" + DIR_SEPARATOR;
	//		boolean linux = false;
	//		if (os.contains("WIN")) {
	//			sysProp += "chromedriver-win.exe";
	//		} else if (os.contains("MAC")) {
	//			sysProp += "chromedriver-mac";
	//		} else {
	//			linux = true;
	//			sysProp += "chromedriver-linux";
	//		}
	//		System.setProperty("webdriver.chrome.driver", sysProp);
	//		boolean secure = url.contains("https");
	//
	//		ChromeOptions chromeOptions = new ChromeOptions();
	//		String customGoogleBinaryLocation = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_BINARY);
	//		if (customGoogleBinaryLocation != null && !customGoogleBinaryLocation.isEmpty()) {
	//			chromeOptions.setBinary(customGoogleBinaryLocation);
	//		}
	//		chromeOptions.addArguments("--headless");
	//		chromeOptions.addArguments("--disable-gpu");
	//		chromeOptions.addArguments("--window-size=" + height + "," + width);
	//		chromeOptions.addArguments("--remote-debugging-port=9222");
	//		if (linux) {
	//			chromeOptions.addArguments("-disable-dev-shm-usage");
	//			chromeOptions.addArguments("--no-sandbox");
	//		}
	//		if (url.contains("localhost") && url.contains("https")) {
	//			chromeOptions.addArguments("--allow-insecure-localhost ");
	//		}
	//		driver = new ChromeDriver(chromeOptions);
	//		// driver.manage().timeouts().implicitlyWait(10,TimeUnit.SECONDS) ;
	//
	//		// need to go to the base url first
	//		// so that the cookie is applied at root level
	//		if (ChromeDriverUtility.contextPath != null) {
	//			String startingUrl = feUrl;
	//			if (startingUrl.endsWith("/")) {
	//				startingUrl = startingUrl.substring(0, startingUrl.length() - 1);
	//			}
	//			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/") + 1)
	//					+ ChromeDriverUtility.contextPath;
	//			driver.get(baseUrl);
	//		} else {
	//			driver.get(url);
	//		}
	//		if (sessionId != null && ChromeDriverUtility.sessionCookie != null) {
	//			// name, value, domain, path, expiration, secure, http only
	//			//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, null, "/", null, secure, true);
	//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
	//
	//			driver.manage().addCookie(name);
	//		}
	//		driver.navigate().to(url);
	//
	//		// time for FE to render the page before the image is taken
	//		try {
	//			Thread.sleep(10_000);
	//		} catch (InterruptedException e) {
	//			e.printStackTrace();
	//		}
	//		// take image
	//		File scrFile = (File) ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
	//		try {
	//			FileUtils.copyFile(scrFile, new File(imagePath));
	//		} catch (IOException e) {
	//			e.printStackTrace();
	//		}
	//		if (close) {
	//			driver.quit();
	//		}
	//		return driver;
	//	}

	public static void setContextPath(String contextPath) {
		if (contextPath.startsWith("/")) {
			contextPath = contextPath.substring(1);
		}
		if (contextPath.endsWith("/")) {
			contextPath = contextPath.substring(0, contextPath.length() - 1);
		}
		TCPChromeDriverUtility.contextPath = contextPath;
	}

	public static void setSessionCookie(String sessionCookie) {
		TCPChromeDriverUtility.sessionCookie = sessionCookie;
	}

	public static String getHTML(Object driverObj, String path)
	{
		WebElement we = TCPChromeDriverUtility.driver.findElement(By.xpath(path));
		String html2 = TCPChromeDriverUtility.driver.executeScript("return arguments[0].outerHTML;", we) + "";
		return html2;
	}

	public static void quit(Object driverObj)
	{
		if(driver != null)
			driver.quit();
		driver = null;
	}


//	public static void main(String[] args) {
//		TestUtilityMethods.loadDIHelper();
//		ChromeDriver driver = (ChromeDriver)TCPChromeDriverUtility.makeChromeDriver("https://www.buzzfeed.com/hbraga/best-gifts-2020",
//				"https://www.buzzfeed.com/hbraga/best-gifts-2020", 30, 40);
//		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
//		String eTitle = "Demo Guru99 Page";
//		String aTitle = "";
//		logger.info("Starting wait");
//		// launch Chrome and redirect it to the Base URL
//		driver.get("http://demo.guru99.com/test/guru99home/");
//		// Maximizes the browser window
//		driver.manage().window().maximize();
//		// get the actual value of the title
//		aTitle = driver.getTitle();
//		// compare the actual title with the expected title
//		logger.info("Title is " + aTitle);
//	}

}
