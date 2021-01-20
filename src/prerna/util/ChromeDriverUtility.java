package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import prerna.om.ThreadStore;
import prerna.test.TestUtilityMethods;

public class ChromeDriverUtility {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static String contextPath = null;
	private static String sessionCookie = null;
	private static ChromeDriver driver = null;

	public static void captureImage(String feUrl, String url, String imagePath, String sessionId) {
		captureImage(feUrl, url, imagePath, sessionId, 1920, 1080, true);
	}

	public static ChromeDriver makeChromeDriver(String feUrl, String url, int height, int width) {
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
		if (linux) {
			chromeOptions.addArguments("-disable-dev-shm-usage");
			chromeOptions.addArguments("--no-sandbox");
		}
		if (url.contains("localhost") && url.contains("https")) {
			chromeOptions.addArguments("--allow-insecure-localhost ");
		}
		ChromeDriver newdriver = new ChromeDriver(chromeOptions);

		return newdriver;

	}

	public static void captureImagePersistent(ChromeDriver driver, String feUrl, String url, String imagePath,
			String sessionId) {
		// need to go to the base url first
		// so that the cookie is applied at root level
		driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);

		if (ChromeDriverUtility.contextPath != null) {
			String startingUrl = feUrl;
			if (startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length() - 1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/") + 1)
					+ ChromeDriverUtility.contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}

		if (sessionId != null && ChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration
//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, feUrl, "/", null);
			updateCookie(driver, ChromeDriverUtility.sessionCookie, sessionId);
			String route = ThreadStore.getRouteId();
			if(route != null && !route.isEmpty()) {
				String routeCookieName = DIHelper.getInstance().getProperty(Constants.MONOLITH_ROUTE);
				if (routeCookieName != null && !routeCookieName.isEmpty()) {
					updateCookie(driver, routeCookieName, route);
				}
				
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
		 * ""; //System.out.println(html2);
		 */

		// time for FE to render the page before the image is taken
		try {
			Thread.sleep(800);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// take image
		File scrFile = (File) ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
		try {
			FileUtils.copyFile(scrFile, new File(imagePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static void updateCookie(ChromeDriver driver, String cookieName, String cookieValue)

	{
		Iterator<Cookie> cooki = driver.manage().getCookies().iterator();
		
		// remove if such a cookie exists
		while (cooki.hasNext())
		{
			Cookie cook = (Cookie) cooki.next();
			String name = cook.getName();
			if (name.equalsIgnoreCase(cookieName))
				driver.manage().deleteCookie(cook);
		}

		Cookie name = new Cookie(cookieName, cookieValue, "/"); // , null);
		
		// works - but doesnt login
		driver.manage().addCookie(name);

	}

	public static void captureDataPersistent(ChromeDriver driver, String feUrl, String url, String sessionId) {
		// need to go to the base url first
		// so that the cookie is applied at root level
		// driver.manage().timeouts().implicitlyWait(10,TimeUnit.SECONDS) ;
		if (ChromeDriverUtility.contextPath != null) {
			String startingUrl = feUrl;
			if (startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length() - 1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/") + 1)
					+ ChromeDriverUtility.contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}
		driver.manage().timeouts().implicitlyWait(30, TimeUnit.SECONDS);
		
		if (sessionId != null && ChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration
//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, feUrl, "/", null);
			updateCookie(driver, ChromeDriverUtility.sessionCookie, sessionId);
			String route = ThreadStore.getRouteId();
			if(route != null && !route.isEmpty()) {
				String routeCookieName = DIHelper.getInstance().getProperty(Constants.MONOLITH_ROUTE);
				if (routeCookieName != null && !routeCookieName.isEmpty()) {
					updateCookie(driver, routeCookieName, route);
				}
				
			}
			//Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
			//driver.manage().addCookie(name);
		}

		driver.navigate().to(url);
		
		// add a sleep
		try {
			Thread.sleep(5_000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Capture the image of from a url
	 * 
	 * @param feUrl     the base semoss url
	 * @param url       the insight embed url
	 * @param imagePath location to save image
	 * @param sessionId user session id if logged in
	 */
	public static ChromeDriver captureImage(String feUrl, String url, String imagePath, String sessionId, int height,
			int width, boolean close) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		// load driver options
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
		boolean secure = url.contains("https");

		ChromeOptions chromeOptions = new ChromeOptions();
		String customGoogleBinaryLocation = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_BINARY);
		if (customGoogleBinaryLocation != null && !customGoogleBinaryLocation.isEmpty()) {
			chromeOptions.setBinary(customGoogleBinaryLocation);
		}
		chromeOptions.addArguments("--headless");
		chromeOptions.addArguments("--disable-gpu");
		chromeOptions.addArguments("--window-size=" + height + "," + width);
		chromeOptions.addArguments("--remote-debugging-port=9222");
		if (linux) {
			chromeOptions.addArguments("-disable-dev-shm-usage");
			chromeOptions.addArguments("--no-sandbox");
		}
		if (url.contains("localhost") && url.contains("https")) {
			chromeOptions.addArguments("--allow-insecure-localhost ");
		}
		driver = new ChromeDriver(chromeOptions);
		// driver.manage().timeouts().implicitlyWait(10,TimeUnit.SECONDS) ;

		// need to go to the base url first
		// so that the cookie is applied at root level
		if (ChromeDriverUtility.contextPath != null) {
			String startingUrl = feUrl;
			if (startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length() - 1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/") + 1)
					+ ChromeDriverUtility.contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}
		if (sessionId != null && ChromeDriverUtility.sessionCookie != null) {
			// name, value, domain, path, expiration, secure, http only
//			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, null, "/", null, secure, true);
			Cookie name = new Cookie(ChromeDriverUtility.sessionCookie, sessionId, "/");
		
			driver.manage().addCookie(name);
		}
		driver.navigate().to(url);

		// time for FE to render the page before the image is taken
		try {
			Thread.sleep(10_000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// take image
		File scrFile = (File) ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
		try {
			FileUtils.copyFile(scrFile, new File(imagePath));
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (close) {
			driver.quit();
		}
		return driver;
	}

	public static void setContextPath(String contextPath) {
		if (contextPath.startsWith("/")) {
			contextPath = contextPath.substring(1);
		}
		if (contextPath.endsWith("/")) {
			contextPath = contextPath.substring(0, contextPath.length() - 1);
		}
		ChromeDriverUtility.contextPath = contextPath;
	}

	public static void setSessionCookie(String sessionCookie) {
		ChromeDriverUtility.sessionCookie = sessionCookie;
	}

	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();
		ChromeDriver driver = ChromeDriverUtility.makeChromeDriver("https://www.buzzfeed.com/hbraga/best-gifts-2020",
				"https://www.buzzfeed.com/hbraga/best-gifts-2020", 30, 40);
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
		String eTitle = "Demo Guru99 Page";
		String aTitle = "";
		System.out.println("Starting wait");
		// launch Chrome and redirect it to the Base URL
		driver.get("http://demo.guru99.com/test/guru99home/");
		// Maximizes the browser window
		driver.manage().window().maximize();
		// get the actual value of the title
		aTitle = driver.getTitle();
		// compare the actual title with the expected title
		System.out.println("Title is " + aTitle);
	}

}
