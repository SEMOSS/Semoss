package prerna.sablecc2.reactor.frame.r.util;

import java.io.File;

import prerna.ds.py.PyExecutorThread;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;

public class PySourceReactor extends AbstractReactor {

	public PySourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);
		String path = getBaseFolder() + "/Py/" + relativePath;
		
		PyExecutorThread py = this.insight.getPy();
		
		Object monitor = py.getMonitor();
		
		File file = new File(path);
		
		// if the file is not there try in the insight
		if(!file.exists())
			path = insight.getInsightFolder() + "/" + relativePath;
		path = path.replace("\\", "/");
		
		file = new File(path);
		String name = file.getName();
		name = name.replaceAll(".py", "");
		synchronized(monitor)
		{
			try {
				String []  commands = new String[]{"import smssutil",name + " = smssutil.loadScript(\"smss\", \"" + path + "\")"};
				py.command = commands;
				monitor.notify();
				monitor.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
     * Get the base folder
     * @return
     */
     protected String getBaseFolder() {
          String baseFolder = null;
          try {
              baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
          } catch (Exception ignored) {
              //logger.info("No BaseFolder detected... most likely running as test...");
          }
          return baseFolder;
     }
}
