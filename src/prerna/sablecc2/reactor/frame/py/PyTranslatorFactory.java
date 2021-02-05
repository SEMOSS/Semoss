package prerna.sablecc2.reactor.frame.py;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	
	private PyTranslatorFactory()
	{
		
	}
	
	public static PyTranslator getTranslator()
	{
		
		if(pyt == null)
		{
			// all of the logic should go here now ?
			//synchronized(this)
			{
				if (PyUtils.pyEnabled()) 
				{
		
					boolean useFilePy = DIHelper.getInstance().getProperty("USE_PY_FILE") != null
							&& DIHelper.getInstance().getProperty("USE_PY_FILE").equalsIgnoreCase("true");
					boolean useTCP = DIHelper.getInstance().getProperty("USE_TCP_PY") != null
							&& DIHelper.getInstance().getProperty("USE_TCP_PY").equalsIgnoreCase("true");
					useTCP = useFilePy; // forcing it to be same as TCP
		
					if (!useFilePy) {
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
					else if (useFilePy) {
						if (useTCP) {
							String port = DIHelper.getInstance().getProperty("FORCE_PORT"); // this means someone has
																							// started it for debug
							if (port == null) {
								port = Utility.findOpenPort();
								pyTupleSpace = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER); // + java.nio.file.FileSystems.getDefault().getSeparator() + "temp";
								pyTupleSpace = PyUtils.getInstance().startPyServe(nouser, pyTupleSpace, port); // make the user dummy
								nc = new NettyClient();
								nc.connect("127.0.0.1", Integer.parseInt(port), false);
								//nc.run();
								Thread t = new Thread(nc);
								t.start();
								int logSleeper = 1;
								while(!nc.isReady() && logSleeper < 6)
								{
									try {
										// wait for it to start
										Thread.sleep(logSleeper*1000);
										logSleeper++;
									} catch (InterruptedException e) {
										logger.error(Constants.STACKTRACE, e);
									}
								}
								pyt = new TCPPyTranslator();
								((TCPPyTranslator) pyt).nc = nc;
							}
						} else {
							PyUtils.getInstance().getTempTupleSpace(nouser, DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR));
							pyt = new FilePyTranslator();
						}
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
