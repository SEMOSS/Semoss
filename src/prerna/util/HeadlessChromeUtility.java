package prerna.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.Cookie;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;

public class HeadlessChromeUtility {

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	/**
	 * Capture the image of from a url
	 * @param url
	 * @param imagePath
	 * @param sessionId
	 */
	public static void captureImage(String url, String imagePath, String sessionId, String contextPath) {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		// get chrome settings
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
		chromeOptions.addArguments("--headless");
		chromeOptions.addArguments("--disable-gpu");
		chromeOptions.addArguments("--window-size=1440,1440");
		if(linux) {
			chromeOptions.addArguments("-disable-dev-shm-usage");
			chromeOptions.addArguments("--no-sandbox");
		}
		if(url.contains("localhost") && url.contains("https")) {
			chromeOptions.addArguments("--allow-insecure-localhost ");
		}
		ChromeDriver driver = new ChromeDriver(chromeOptions);

		
		// need to go to the base url first
		// so that the cookie is applied at root level
		if(contextPath != null) {
			String startingUrl = url;
			if(startingUrl.endsWith("/")) {
				startingUrl = startingUrl.substring(0, startingUrl.length()-1);
			}
			String baseUrl = startingUrl.substring(0, startingUrl.lastIndexOf("/")+1) + contextPath;
			driver.get(baseUrl);
		} else {
			driver.get(url);
		}
		if(sessionId != null) {
			Cookie name = new Cookie(DIHelper.getInstance().getLocalProp(Constants.SESSION_ID_KEY).toString(), sessionId, "/");
			driver.manage().addCookie(name);
		}
		driver.navigate().to(url);
		
		// time for FE to render the page before the image is taken
	    try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		File scrFile = (File)((TakesScreenshot)driver).getScreenshotAs(OutputType.FILE);
		try {
			FileUtils.copyFile(scrFile, new File(imagePath));
		} catch (IOException e) {
			e.printStackTrace();
		}

	    driver.quit();
	}
}
