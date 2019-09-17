package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyUtils;
import prerna.sablecc.ReactorSecurityManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class PyReactor extends AbstractReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = PyReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
		ReactorSecurityManager tempManager = new ReactorSecurityManager();
		tempManager.addClass(CLASS_NAME);
		System.setSecurityManager(tempManager);
		
		Logger logger = getLogger(CLASS_NAME);
		PyExecutorThread pyThread = this.insight.getPy();
		Object lock = pyThread.getMonitor();

		int size = 3;
		// will always have an insight path
		String removePathVariables = "del ROOT";
		
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";

		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;
		
		insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
		insightRootAssignment = "ROOT = \"" + insightRootPath + "\"";
		
		if(this.insight.isSavedInsight()) {
			appRootPath = this.insight.getAppFolder();
			appRootPath = appRootPath.replace('\\', '/');
			appRootAssignment = "APP_ROOT = \"" + appRootPath + "\"";
			removePathVariables += ", APP_ROOT";
			size++;
		}
		try {
			userRootPath = AssetUtility.getAssetBasePath(this.insight, "USER");
			userRootPath = userRootPath.replace('\\', '/');
			userRootAssignment = "USER_ROOT = \"" + userRootPath + "\"";
			removePathVariables += ", USER_ROOT";
			size++;
		} catch(Exception ignore) {
			// ignore
		}
		
		String[] commands = new String[size];
		int counter = 0;
		commands[counter++] = insightRootAssignment;
		if(appRootAssignment != null && !appRootAssignment.isEmpty()) {
			commands[counter++] = appRootAssignment;
		}
		if(userRootAssignment != null && !userRootAssignment.isEmpty()) {
			commands[counter++] = userRootAssignment;
		}
		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		commands[counter++] = code;
		commands[counter++] = removePathVariables;
		
		logger.info("Execution python script: " + code);
		pyThread.command = commands;
		
		Object output = "";
		synchronized(lock) {
			try {
				lock.notify();
				lock.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// waking up now
			output = pyThread.response.get(code);
		}
		
		// clean up the output
		// clean up the output
		if(userRootPath != null && output.toString().contains(userRootPath)) {
			output = output.toString().replace(userRootPath, "$USER_IF");
		}
		if(appRootPath != null && output.toString().contains(appRootPath)) {
			output = output.toString().replace(appRootPath, "$APP_IF");
		}
		if(insightRootPath != null && output.toString().contains(insightRootPath)) {
			output = output.toString().replace(insightRootPath, "$IF");
		}
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		
		// set back the original security manager
		tempManager.removeClass(CLASS_NAME);
		System.setSecurityManager(defaultManager);	
		
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}
