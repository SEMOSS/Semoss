package prerna.sablecc2.reactor.frame.py;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.py.FilePyTranslator;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.pyserve.NettyClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyTranslatorFactory {
	
	// singleton to give you a py reactor when no user is there
	public static PyTranslator pyt = null;
	public static NettyClient nc = null;
	private static Logger logger = LogManager.getLogger(PyTranslatorFactory.class);
	public static String nouser = "nouser";
	public static String pyTupleSpace = null;
	public static NettyClient pyServe;

	
	private PyTranslatorFactory()
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
				if (AbstractSecurityUtils.securityEnabled() && PyUtils.pyEnabled()) 
				{
		
					//boolean useFilePy = DIHelper.getInstance().getProperty("USE_PY_FILE") != null
					//		&& DIHelper.getInstance().getProperty("USE_PY_FILE").equalsIgnoreCase("true");
//					boolean useTCP = DIHelper.getInstance().getProperty("USE_TCP_PY") != null
//							&& DIHelper.getInstance().getProperty("USE_TCP_PY").equalsIgnoreCase("true");
					boolean useNettyPy = DIHelper.getInstance().getProperty("NETTY_PYTHON") != null
							&& DIHelper.getInstance().getProperty("NETTY_PYTHON").equalsIgnoreCase("true");
					boolean useNettyR = DIHelper.getInstance().getProperty("NETTY_R") != null
							&& DIHelper.getInstance().getProperty("NETTY_R").equalsIgnoreCase("true");
	//				useTCP = useNetty ; // forcing it to be same as TCP // if it is netty start it up dont use a py thread
					//if(!useFilePy)	
					//	useFilePy = useNetty;
					boolean useNetty = useNettyPy || useNettyR;
					
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
					// check to see if the py translator needs to be set ?
					// check to see if the py translator needs to be set ?
					if (useNetty) {
						if (pyServe == null)  // start only if it not already in progress
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
								pyTupleSpace = PyUtils.getInstance().startPyServe(nouser, pyTupleSpace, port);
							}
							
							NettyClient nc = new NettyClient();
							pyServe = nc;

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
							if(useNettyPy)
							{
								pyt = new TCPPyTranslator();
								((TCPPyTranslator) pyt).nc = nc;
							}
						}
						// not sure this is valid anymore
						/*
						else {
							PyUtils.getInstance().getTempTupleSpace(this, DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR));
							pyt = new FilePyTranslator();
						}
						*/
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
			if (pyt instanceof FilePyTranslator)
				PyUtils.getInstance().killTempTupleSpace(nouser);
			if (pyt instanceof TCPPyTranslator) {
				String dir = pyTupleSpace;
				nc.stopPyServe(dir);
			}
		}

	}
	

}
