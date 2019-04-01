package prerna.engine.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.om.InsightCacheUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SaveInsightIntoWorkspace {

	private static Boolean SAVE_USER_WORKSPACE = null;
	
	/*
	 * Class that uses a queue to save the most recent version of an insight 
	 * into the user workspace app
	 */
	
	private IEngine userWorkspaceEngine;
	private InsightAdministrator insightAdmin;
	private String workspaceSavedInsightId;
	
	private InsightCacher cacher;
	private BlockingQueue<List<String>> queue = null;
	private String insightName;
	private Thread t;
	
	public SaveInsightIntoWorkspace(String userWorkspaceId, String rdbmsId, String insightName, boolean created) {
		this.userWorkspaceEngine = Utility.getEngine(userWorkspaceId);
		this.insightAdmin = new InsightAdministrator(this.userWorkspaceEngine.getInsightDatabase());
//		if(created) {
//			this.workspaceSavedInsightId = rdbmsId;
//		} else {
			this.workspaceSavedInsightId = UUID.randomUUID().toString();
//		}
		this.queue = new ArrayBlockingQueue<List<String>>(50);
		this.insightName = insightName;
		
		this.cacher = new InsightCacher(this.workspaceSavedInsightId, this.queue, 
				this.userWorkspaceEngine, this.insightAdmin, 
				this.insightName);
//		this.cacher.setCreated(created);
		
		this.t = new Thread(this.cacher);
		this.t.start();
	}
	
	/**
	 * Adds a new pixel into the queue that is running for this insight
	 * @param pixelSteps
	 */
	public void addToQueue(List<String> pixelSteps) {
		queue.add(pixelSteps);
	}
	
	/**
	 * Drop the insight from the workspace app
	 */
	public void dropWorkspaceCache() {
		this.insightAdmin.dropInsight(this.workspaceSavedInsightId);
		SecurityInsightUtils.deleteInsight(this.userWorkspaceEngine.getEngineId(), this.workspaceSavedInsightId);
	}
	
	/**
	 * Kill the thread running in the background that
	 * is picking up new recipe steps being added to the queue
	 */
	public void killThread() {
		this.cacher.kill();
	}

	public void setInsightName(String insightName) {
		this.insightName = insightName;
		this.cacher.setInsightName(insightName);
	}
	
	public static boolean isCacheUserWorkspace() {
		if(SAVE_USER_WORKSPACE == null) {
			SAVE_USER_WORKSPACE = Boolean.parseBoolean(DIHelper.getInstance().getProperty("USER_WORKSPACE"));
		}
		return SAVE_USER_WORKSPACE;
	}
}

class InsightCacher implements Runnable {

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd h:mm:ss a");

	private BlockingQueue<List<String>> queue;

	private String workspaceAppId;
	private String workspaceAppName;
	private InsightAdministrator insightAdmin;
	
	private String workspaceSavedInsightId;
	private String insightName;
	private boolean created = false;

	private boolean notKill = true;
	
	public InsightCacher(String workspaceSavedInsightId, BlockingQueue<List<String>> queue, IEngine worksapceEngine, InsightAdministrator insightAdmin, String insightName) {
		this.workspaceSavedInsightId = workspaceSavedInsightId;
		this.queue = queue;
		
		this.workspaceAppId = worksapceEngine.getEngineId();
		this.workspaceAppName = worksapceEngine.getEngineName();
		this.insightAdmin = insightAdmin;
		
		setInsightName(insightName);
	}

	@Override
	public void run() {
		try {
			List<String> lastPixel = null;
			while( notKill && (lastPixel = queue.take()) != null) {
				if(lastPixel.isEmpty()) {
					continue;
				}
				if(created) {
					// update the existing insight
					String inName = insightName + " " + formatter.format(new Date());
					insightAdmin.updateInsight(this.workspaceSavedInsightId, inName, "default", lastPixel, false);
					SecurityInsightUtils.updateInsight(this.workspaceAppId, this.workspaceSavedInsightId, inName, true, "default");
					// delete the cache if it is there
					InsightCacheUtility.deleteCache(this.workspaceAppId, this.workspaceAppName, this.workspaceSavedInsightId);

				} else {
					// create new
					String inName = insightName + " " + formatter.format(new Date());
					insightAdmin.addInsight(this.workspaceSavedInsightId, inName, "default", lastPixel, false);
					SecurityInsightUtils.addInsight(this.workspaceAppId, this.workspaceSavedInsightId, inName, true, "default");

					created = true;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void kill() {
		this.notKill = false;
		this.queue.clear();
		// to force the queue to take
		this.queue.add(new ArrayList<String>());
	}
	

	public void setInsightName(String insightName) {
		if(insightName == null || insightName.trim().isEmpty()) {
			this.insightName = "Unsaved Insight";
		} else {
			this.insightName = insightName;
			Pattern p = Pattern.compile(".*\\d{4}\\-\\d{2}\\-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\s(A|P)M");
			Matcher m = p.matcher(this.insightName);
			if(m.matches()) {
				this.insightName = this.insightName.substring(0, this.insightName.length()-22).trim();
			}
		}
	}
	
//	public void setCreated(boolean created) {
//		this.created = created;
//	}
}