package prerna.reactor.frame.py;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Settings;
import prerna.util.Utility;

public class PySingleton {
	
	// singleton to give you a py reactor when no user is there
	public static PyTranslator pyt = null;
	private static Logger logger = LogManager.getLogger(PySingleton.class);
	public static User nouser = new User();
	static {
		nouser.setAnonymous(true);
		nouser.setAnonymousId("nouser");
	}
	public static String pyTupleSpace = null;
	public static SocketClient tcpServer;

	
	private PySingleton()
	{
		
	}
	
	public static PyTranslator getTranslator()
	{
		
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is set to false for this instance");
		}

		
		if(pyt == null)
		{
			// all of the logic should go here now ?
			{
				if (PyUtils.pyEnabled()) 
				{
		
					boolean useNettyPy = DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON) != null
							&& DIHelper.getInstance().getProperty(Constants.NETTY_PYTHON).equalsIgnoreCase("true");
					
					if (!useNettyPy) {
						PyExecutorThread jepThread = null;
						if (jepThread == null) {
							jepThread = PyUtils.getInstance().getJep();
							pyt = new PyTranslator();
							pyt.setPy(jepThread);
							int logSleeper = 1;
							while(!jepThread.isReady())
							{
								try {
									// wait for it to start
									Thread.sleep(logSleeper*1000);
									logSleeper++;
								} catch (InterruptedException e) {
									logger.error(Constants.STACKTRACE, e);
								}
							}
							logger.info("Jep Start is Complete");
						}
					}
					else
					{
						getTCPServer();
						pyt = new TCPPyTranslator();
						((TCPPyTranslator) pyt).setSocketClient(tcpServer); // starts it						
					}
				}
			}
		}
		return pyt;

	}
	
	public static void stopPy()
	{
		// stop python too
		if (pyt != null) 
		{
			if (pyt instanceof prerna.ds.py.PyTranslator)
				PyUtils.getInstance().killPyThread(pyt.getPy());
			if (pyt instanceof TCPPyTranslator) {
				String dir = pyTupleSpace;
				tcpServer.stopPyServe(dir);
			}
		}

	}
	
	public static  SocketClient getTCPServer()
	{
		if (tcpServer == null)  // start only if it not already in progress
		{
			String port = DIHelper.getInstance().getProperty("FORCE_PORT"); // this means someone has
																			// started it for debug
			if (port == null) // port has not been forced
			{
				port = Utility.findOpenPort();
				if(DIHelper.getInstance().getProperty("PY_TUPLE_SPACE")!=null && !DIHelper.getInstance().getProperty("PY_TUPLE_SPACE").isEmpty()) {
					pyTupleSpace=(DIHelper.getInstance().getProperty("PY_TUPLE_SPACE"));
				} else {
					pyTupleSpace = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR);
				}
				pyTupleSpace = PyUtils.getInstance().startTCPServe(nouser, pyTupleSpace, port);
			}

			String pyClient = DIHelper.getInstance().getProperty(Settings.TCP_CLIENT);
			if(pyClient == null || (pyClient=pyClient.trim()).isEmpty()) {
				pyClient = "prerna.tcp.client.Client";
			}
			try {
					SocketClient nc = (SocketClient)Class.forName(pyClient).newInstance();
					tcpServer = nc;
	
					nc.connect("127.0.0.1", Integer.parseInt(port), false);
					
					//nc.run(); - you cannot do this because then the client goes into listener mode
					Thread t = new Thread(nc);
					t.start();
	
					while(!nc.isReady())
					{
						synchronized(nc)
						{
							try 
							{
								nc.wait();
								logger.info("Setting the netty client ");
							} catch (InterruptedException e) {
								logger.error(Constants.STACKTRACE, e);
							}								
						}
					}
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return tcpServer;
	}

	

}
