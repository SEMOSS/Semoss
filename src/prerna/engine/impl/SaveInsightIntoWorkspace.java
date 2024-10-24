package prerna.engine.impl;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.cache.InsightCacheUtility;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.Utility;

import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SaveInsightIntoWorkspace {

	private static Boolean SAVE_USER_WORKSPACE = null;
	
	/*
	 * Class that uses a queue to save the most recent version of an insight 
	 * into the user workspace app
	 */
	
	private static final Logger classLogger = LogManager.getLogger(SaveInsightIntoWorkspace.class);

	private IProject userWorkspaceProject;
	private InsightAdministrator insightAdmin;
	private String workspaceSavedInsightId;
	
	private InsightCacher cacher;
	private BlockingQueue<List<String>> queue = null;
	private String insightName;
	private Thread t;
	
	public SaveInsightIntoWorkspace(String userWorkspaceId, String rdbmsId, String insightName, boolean created) {
		this.userWorkspaceProject = Utility.getProject(userWorkspaceId);
		this.insightAdmin = new InsightAdministrator(this.userWorkspaceProject.getInsightDatabase());
		this.workspaceSavedInsightId = UUID.randomUUID().toString();
		this.queue = new ArrayBlockingQueue<List<String>>(50);
		this.insightName = insightName;
		
		this.cacher = new InsightCacher(this.workspaceSavedInsightId, this.queue, 
				this.userWorkspaceProject, this.insightAdmin, 
				this.insightName);
		
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
		SecurityInsightUtils.deleteInsight(this.userWorkspaceProject.getProjectId(), this.workspaceSavedInsightId);
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
			SAVE_USER_WORKSPACE = Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.USER_WORKSPACE));
		}
		return SAVE_USER_WORKSPACE;
	}
}

class InsightCacher implements Runnable {

	private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd h:mm:ss a");

	private static final Logger classLogger = LogManager.getLogger(InsightCacher.class);

	private BlockingQueue<List<String>> queue;

	private String workspaceAppId;
	private String workspaceAppName;
	private InsightAdministrator insightAdmin;
	
	private String workspaceSavedInsightId;
	private String insightName;
	private boolean created = false;

	private boolean notKill = true;
	
	public InsightCacher(String workspaceSavedInsightId, BlockingQueue<List<String>> queue, IProject worksapceEngine, InsightAdministrator insightAdmin, String insightName) {
		this.workspaceSavedInsightId = workspaceSavedInsightId;
		this.queue = queue;
		
		this.workspaceAppId = worksapceEngine.getProjectId();
		this.workspaceAppName = worksapceEngine.getProjectName();
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
				boolean cacheable = true;
				int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
				boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
				String cacheCron = Utility.getApplicationCacheCron();
				ZonedDateTime cachedOn = null;
				Map<String, Object> parameterValues = null;
				
				if(created) {
					// update the existing insight
					String inName = insightName + " " + formatter.format(new Date());
					insightAdmin.updateInsight(this.workspaceSavedInsightId, inName, "default", lastPixel, false, 
							cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, null);
					SecurityInsightUtils.updateInsight(this.workspaceAppId, this.workspaceSavedInsightId, inName, true, 
							"default", cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, lastPixel, null);
					// delete the cache if it is there
					InsightCacheUtility.deleteCache(this.workspaceAppId, this.workspaceAppName, this.workspaceSavedInsightId, parameterValues, true);

				} else {
					// create new
					String inName = insightName + " " + formatter.format(new Date());
					insightAdmin.addInsight(this.workspaceSavedInsightId, inName, "default", lastPixel, false, 
							cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, null);
					SecurityInsightUtils.addInsight(this.workspaceAppId, this.workspaceSavedInsightId, 
							inName, true, "default", cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, lastPixel, null);

					created = true;
				}
			}
		} catch (InterruptedException e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	
}