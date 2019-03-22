package prerna.engine.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.om.InsightCacheUtility;
import prerna.util.Utility;

public class SaveInsightIntoWorkspace {

	private IEngine userWorkspaceEngine;
	private InsightAdministrator insightAdmin;
	private String workspaceSavedInsightId;
	
	private InsightCacher cacher;
	private BlockingQueue<List<String>> queue = null;
	private String insightName;
	private Thread t;
	
	public SaveInsightIntoWorkspace(String userWorkspaceId, String insightName) {
		this.userWorkspaceEngine = Utility.getEngine(userWorkspaceId);
		this.insightAdmin = new InsightAdministrator(this.userWorkspaceEngine.getInsightDatabase());
		this.workspaceSavedInsightId = UUID.randomUUID().toString();

		this.queue = new ArrayBlockingQueue<List<String>>(50);
		this.insightName = insightName;
		
		this.cacher = new InsightCacher(this.workspaceSavedInsightId, this.queue, this.userWorkspaceEngine, insightAdmin, this.insightName);
		
		this.t = new Thread(this.cacher);
		this.t.start();
	}
	
	public void addToQueue(List<String> pixelSteps) {
		queue.add(pixelSteps);
	}
	
	public void dropWorkspaceCache() {
		this.insightAdmin.dropInsight(this.workspaceSavedInsightId);
		SecurityInsightUtils.deleteInsight(this.userWorkspaceEngine.getEngineId(), this.workspaceSavedInsightId);
	}
	
	public void killThread() {
		this.cacher.kill();
	}
}

class InsightCacher implements Runnable {

	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
		
		this.insightName = (insightName == null || insightName.trim().isEmpty()) ? "UnsavedInsight" : insightName.trim();
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
}