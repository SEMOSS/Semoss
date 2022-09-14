package prerna.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.tcp.PayloadStruct;
import prerna.tcp.client.Client;
import prerna.test.TestUtilityMethods;
import prerna.util.insight.InsightUtility;

public class NettyChromeDriverClient extends ChromeDriverUtility{

	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	protected static final Logger logger = LogManager.getLogger(InsightUtility.class.getName());

	private static Client client = null;
	
	private static boolean driverMade = false;

	public static void setClient(Client tcpServer)
	{
		client = tcpServer;
		setContextAndSessionCookie(contextPath, sessionCookie);
	}
	
	public static Object makeChromeDriver(String feUrl, String url, int height, int width) 
	{	
		
		if(client != null)
		{
			if(!driverMade)
			{
				String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
				PayloadStruct ps = constructPayload(methodName, feUrl, url, height, width);
				ps.payloadClasses = new Class[] {String.class, String.class, int.class, int.class};
				ps.hasReturn = false;
				ps = (PayloadStruct)client.executeCommand(ps);
				if(ps != null  &&  ps.ex!= null)
				{
					logger.info(ps.ex);
				}
				//else
				//	driverMade = true;
			}
		}

		return new Object();
	}
		
	
	public static void captureImagePersistent(Object driver, String feUrl, String url, String imagePath,
			String sessionId, int waitTime) 
	{
		if(client != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, driver, feUrl, url, imagePath, sessionId, waitTime);
			ps.payloadClasses = new Class[] {Object.class, String.class, String.class, String.class, String.class, int.class };
			ps = (PayloadStruct)client.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
			else if(ps != null)
				System.err.println("File path is " + ps.payload[0]);
		}
	}

	protected static void updateCookie(Object driver, String cookieName, String cookieValue)
	{
		if(client != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, driver, cookieName, cookieValue);
			ps.payloadClasses = new Class[] {Object.class, String.class, String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)client.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
	}

	public static String captureDataPersistent(Object driver, String feUrl, String url, String sessionId, int waitTime) {
		if(client != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, driver, feUrl, url, sessionId, waitTime);
			ps.payloadClasses = new Class[] {Object.class, String.class, String.class, String.class, int.class};
			ps.hasReturn = false;
			PayloadStruct ps2 = (PayloadStruct)client.executeCommand(ps);
			if(ps2 != null  &&  ps2.ex!= null)
			{
				logger.info(ps2.ex);
			}
			else
				return ps2.payload[0] + "";
		}
		return null;
	}

	public static String getHTML(Object driver, String path)
	{
		if(client != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, driver, path);
			ps.payloadClasses = new Class[] {Object.class, String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)client.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
			else 
			{
				String retHTML = ps.payload[0] + "";
				System.err.println("HTML..  " + retHTML);
				return retHTML;
			}
		}
		
		return null;
	}


	private  static PayloadStruct constructPayload(String methodName, Object...objects )
	{
		// go through the objects and if they are set to null then make them as string null
		PayloadStruct ps = new PayloadStruct();
		ps.operation = PayloadStruct.OPERATION.CHROME;
		ps.methodName = methodName;
		ps.payload = objects;
		ps.longRunning = true;
		
		return ps;
	}
	
	public static void setContextAndSessionCookie(String contextPath, String sessionCookie)
	{
		// main call to set session cookie and context on other side
		if(client != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, contextPath, sessionCookie);
			ps.payloadClasses = new Class[] {String.class, String.class};
			ps.hasReturn = false;
			ps = (PayloadStruct)client.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
	}

	public static void quit(Object driver)
	{
		// main call to set session cookie and context on other side
		if(client != null)
		{
			String methodName = new Object(){}.getClass().getEnclosingMethod().getName();
			PayloadStruct ps = constructPayload(methodName, driver);
			ps.payloadClasses = new Class[] {Object.class};
			ps.hasReturn = false;
			// dont call for now
			// call only when the end happens
			ps = (PayloadStruct)client.executeCommand(ps);
			if(ps != null  &&  ps.ex!= null)
			{
				logger.info(ps.ex);
			}
		}
	}

	
	public static void setSessionCookie(String sessionCookie) {
		NettyChromeDriverClient.sessionCookie = sessionCookie;
	}



	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();
		
		// this wont work for now
		/*
		ChromeDriver driver = NettyChromeDriverUtility.makeChromeDriver("https://www.buzzfeed.com/hbraga/best-gifts-2020",
				"https://www.buzzfeed.com/hbraga/best-gifts-2020", 30, 40);
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
		String eTitle = "Demo Guru99 Page";
		String aTitle = "";
		logger.info("Starting wait");
		// launch Chrome and redirect it to the Base URL
		driver.get("http://demo.guru99.com/test/guru99home/");
		// Maximizes the browser window
		driver.manage().window().maximize();
		// get the actual value of the title
		aTitle = driver.getTitle();
		// compare the actual title with the expected title
		logger.info("Title is " + aTitle);
		*/
	}

}
