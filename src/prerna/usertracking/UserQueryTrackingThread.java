package prerna.usertracking;

import java.time.LocalDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;

public class UserQueryTrackingThread implements Runnable {

	private static final Logger classLogger = LogManager.getLogger(UserQueryTrackingThread.class);
	
	private User user = null;
	private String engineId = null;
	private String query = null;
	private java.sql.Timestamp startTime = null;
	private java.sql.Timestamp endTime = null;
	private boolean failed = false;
	
	/**
	 * 
	 * @param user
	 * @param engineId
	 */
	public UserQueryTrackingThread(User user, String engineId) {
		this.user = user;
		this.engineId = engineId;
	}
	
	@Override
	public void run() {
		Long executionTime = null;
		if(endTime != null) {
			executionTime = endTime.getTime() - startTime.getTime();
		}
		if(this.startTime == null) {
			classLogger.warn("Storing query execution without a start time.");
		}
		UserTrackingUtils.trackQueryExecution(user, engineId, query, 
				startTime, endTime, 
				executionTime, failed);
	}
	
	public void setQuery(String query) {
		this.query = query;
	}
	
	public void setFailed() {
		this.failed = true;
	}
	
	public void setStartTimeNow() {
		this.startTime = java.sql.Timestamp.valueOf(LocalDateTime.now());
	}

	public void setEndTimeNow() {
		this.endTime = java.sql.Timestamp.valueOf(LocalDateTime.now());
	}
	
}
