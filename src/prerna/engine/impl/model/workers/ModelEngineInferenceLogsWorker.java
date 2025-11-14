package prerna.engine.impl.model.workers;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;

import com.google.gson.GsonBuilder;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.project.api.IProject;
import prerna.util.Utility;

public class ModelEngineInferenceLogsWorker implements Runnable {

	public static final String INPUT = "INPUT";
	public static final String RESPONSE = "RESPONSE";

	private String messageId;
	private String transactionId;
	private String messageMethod;
	private IEngine engine;
	private String insightId;
	private String projectContextId;
	private String projectId;
	private User user;
	private String sessionId;
	private String roomId;
	private String context;
	private String prompt;
	private Object fullPrompt;
	private Integer promptTokens;
	private ZonedDateTime inputTime;
	private String response;
	private Integer responseTokens;
	private ZonedDateTime responseTime;

	// @formatter:off
    public ModelEngineInferenceLogsWorker(
		String messageId, 
		String transactionId,
		String messageMethod, 
		IEngine engine,
		String insightId,
	    String projectContextId,
	    String projectId,
	    User user,
		String sessionId,
		String roomId,
	   	String context,
	   	String prompt,
	   	Object fullPrompt,
	   	Integer promptTokens,
	   	ZonedDateTime inputTime,
	   	String response,
	   	Integer responseTokens,
	   	ZonedDateTime responseTime
	) {
    	this.messageId = messageId;
    	this.transactionId = transactionId;
    	this.messageMethod = messageMethod;
    	this.engine = engine;
    	this.insightId = insightId;
    	this.projectContextId = projectContextId;
    	this.projectId = projectId;
    	this.user = user;
    	this.sessionId = sessionId;
    	this.roomId = roomId;
    	this.context = context;
        this.prompt = prompt;
        if(this.prompt != null) {
        	this.prompt = this.prompt.trim();
        }
        this.fullPrompt = fullPrompt;
        this.promptTokens = promptTokens;
        this.inputTime = inputTime;
        this.response = response;
        this.responseTokens = responseTokens;
        this.responseTime = responseTime;
    }
    // @formatter:on

	@Override
	public void run() {
		String agentType = engine.getCatalogSubType(engine.getSmssProp());

		// assumption, if project level, then they will be inferencing through a saved
		// insight or SetContext
		String projectId = this.projectContextId;
		if (projectId == null) {
			projectId = this.projectId;
		}
		String projectName = null;
		if (projectId != null) {
			IProject project = Utility.getProject(projectId);
			projectName = project.getProjectName();
		}

		AccessToken userToken = user.getPrimaryLoginToken();
		String userId = userToken.getId();
		String userName = userToken.getName();
		String userUsername = userToken.getUsername();
		String userEmail = userToken.getEmail();

		// try to get the user's actual name otherwise try for username or email address
		if (userName == null) {
			if (userUsername != null) {
				userName = userUsername;
			} else {
				userName = userEmail;
			}
		}

		if (prompt == null || prompt.isEmpty()) {
			if (fullPrompt instanceof Collection && ((Collection) fullPrompt).size() == 1) {
				Object promptObj = ((Collection) fullPrompt).iterator().next();
				if (promptObj instanceof String) {
					prompt = (String) promptObj;
				} else {
					prompt = new GsonBuilder().disableHtmlEscaping().create().toJson(promptObj);
				}
			} else {
				prompt = new GsonBuilder().disableHtmlEscaping().create().toJson(fullPrompt);
			}
		}

		Duration duration = Duration.between(inputTime, responseTime);
		long millisecondsDifference = duration.toMillis();
		Double millisecondsDouble = (double) millisecondsDifference;

		// TODO this needs to be moved to wherever we "publish" a new LLM/agent
		if (!ModelInferenceLogsUtils.doModelIsRegistered(engine.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(engine.getEngineId(), engine.getEngineName(), null, agentType,
					user.getPrimaryLoginToken().getId());
		}

		// @formatter:off
		if (!ModelInferenceLogsUtils.doCheckRoomExists(roomId)) {
			String roomName = prompt.substring(0, Math.min(prompt.length(), 100));
			ModelInferenceLogsUtils.doCreateNewConversation(
				insightId,
				roomId,
				roomName, 
				null, 
				userId,
				userName,
				userEmail,
				agentType,
				engine.getEngineId(),
				true, 
				projectId, 
				projectName,
				null
			);
		}
		// @formatter:on

		if (this.context != null) {
			// set the context for the room / insight
			ModelInferenceLogsUtils.setRoomContext(roomId, userId, context);
		}

		boolean keepInputOutput = false;
		// TODO: ADD TO INTERFACE SO NOT DOING THIS DUMB CASTING
		if (engine instanceof AbstractModelEngine) {
			keepInputOutput = ((AbstractModelEngine) engine).keepInputOutput();
		} else if (engine instanceof AbstractVectorDatabaseEngine) {
			keepInputOutput = ((AbstractVectorDatabaseEngine) engine).keepInputOutput();
		} else if (engine instanceof PGVectorDatabaseEngine) {
			keepInputOutput = ((PGVectorDatabaseEngine) engine).keepInputOutput();
		}

		// @formatter:off
		if(keepInputOutput) {
			ModelInferenceLogsUtils.doRecordMessage(
				this.messageId,
				this.transactionId,
				INPUT,
				this.prompt,
				this.messageMethod,
				this.promptTokens,
				millisecondsDouble,
				this.inputTime,
				this.engine.getEngineId(),
				insightId,
				this.sessionId,
				this.roomId,
				userId,
				userName,
				userEmail
			);
			ModelInferenceLogsUtils.doRecordMessage(
				this.transactionId,
				this.transactionId,
				RESPONSE,
				this.response,
				this.messageMethod,
				this.responseTokens,
				millisecondsDouble,
				this.responseTime,
				this.engine.getEngineId(),
				insightId,
				this.sessionId,
				this.roomId,
				userId,
				userName,
				userEmail
			);
		} else {
			ModelInferenceLogsUtils.doRecordMessage(
				this.messageId,
				this.transactionId, 
				INPUT,
				null,
				this.messageMethod,
				this.promptTokens,
				millisecondsDouble,
				this.inputTime,
				this.engine.getEngineId(),
				insightId,
				this.sessionId,
				this.roomId,
				userId,
				userName,
				userEmail
			);
			ModelInferenceLogsUtils.doRecordMessage(
				this.transactionId,
				this.transactionId,
				RESPONSE,
				null,
				this.messageMethod,
				this.responseTokens,
				millisecondsDouble,
				this.responseTime,
				this.engine.getEngineId(),
				insightId,
				this.sessionId,
				this.roomId,
				userId,
				userName,
				userEmail
			);
		}
		// @formatter:on
	}
}
