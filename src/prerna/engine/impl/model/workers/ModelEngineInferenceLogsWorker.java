package prerna.engine.impl.model.workers;

import java.time.Duration;
import java.time.ZonedDateTime;

import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.job.JobReactor;
import prerna.util.Utility;

public class ModelEngineInferenceLogsWorker implements Runnable {
	
	public static final String INPUT = "INPUT";
	public static final String RESPONSE = "RESPONSE";
	
	private String messageId;
	private String messageMethod;
    private AbstractModelEngine engine;
    private Insight insight;
    private String prompt;
    private Object fullPrompt;
    private String context;
    private ZonedDateTime inputTime;
    private String response;
    private ZonedDateTime responseTime;
    private Integer promptTokens;
    private Integer responseTokens;
    
    public ModelEngineInferenceLogsWorker(
		String messageId, 
		String messageMethod, 
		AbstractModelEngine engine,
		Insight insight, 
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
    	this.messageMethod = messageMethod;
    	this.engine = engine;
    	this.insight = insight;
    	this.context = context;
        this.prompt = prompt;
        this.fullPrompt = fullPrompt;
        this.promptTokens = promptTokens;
        this.inputTime = inputTime;
        this.response = response;
        this.responseTokens = responseTokens;
        this.responseTime = responseTime;
    }

    @Override
    public void run() {
    	String sessionId = null;
		if (insight.getVarStore().containsKey(JobReactor.SESSION_KEY)) {
			sessionId = (String) insight.getVarStore().get(JobReactor.SESSION_KEY).getValue();
		}
		
		// assumption, if project level, then they will be inferencing through a saved insight or SetContext
		String projectId = insight.getContextProjectId();
		if (projectId == null) {
			projectId = insight.getProjectId();
		}
		String projectName = null;
		if (projectId != null) {
			IProject project = Utility.getProject(projectId);
			projectName = project.getProjectName();
		}
		
		String insightId = insight.getInsightId();
		
		User user = insight.getUser();
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
				
		if (prompt == null) {
			prompt = new Gson().toJson(fullPrompt);
		} else {
			prompt = prompt.replace("'", "\'");
		}
		
        Duration duration = Duration.between(inputTime, responseTime);
        long millisecondsDifference = duration.toMillis();
        Double millisecondsDouble = (double) millisecondsDifference;
        
		// TODO this needs to be moved to wherever we "publish" a new LLM/agent
		if (!ModelInferenceLogsUtils.doModelIsRegistered(engine.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(engine.getEngineId(), engine.getEngineName(), null, 
					engine.getModelType().toString(), user.getPrimaryLoginToken().getId());
		}
		
		if (!ModelInferenceLogsUtils.doCheckConversationExists(insightId) && this.messageMethod.equals("ask")) {
			String roomName = prompt.substring(0, Math.min(prompt.length(), 100));
		
			ModelInferenceLogsUtils.doCreateNewConversation(
				insightId, 
				roomName, 
				null, 
				userId,
				userName,
				engine.getModelType().toString(), 
				true, 
				projectId, 
				projectName, 
				engine.getEngineId()
			);
		}
		
		if (this.context != null) {
			// set the context for the room / insight
			ModelInferenceLogsUtils.setRoomContext(insightId, userId, userName);
		}
				
		if(engine.keepInputOutput()) {
			ModelInferenceLogsUtils.doRecordMessage(
				messageId, 
				INPUT,
				prompt,
				this.messageMethod,
				promptTokens,
				millisecondsDouble,
				inputTime,
				engine.getEngineId(),
				insightId,
				sessionId,
				userId,
				userName
			);
			ModelInferenceLogsUtils.doRecordMessage(
				messageId, 
				RESPONSE,
				response.replace("'", "\'"),
				this.messageMethod,
				responseTokens,
				millisecondsDouble,
				responseTime,
				engine.getEngineId(),
				insightId,
				sessionId,
				userId,
				userName
			);
		} else {
			ModelInferenceLogsUtils.doRecordMessage(
				messageId, 
				INPUT,
				null,
				this.messageMethod,
				promptTokens,
				millisecondsDouble,
				inputTime,
				engine.getEngineId(),
				insightId,
				sessionId,
				userId,
				userName
			);
			ModelInferenceLogsUtils.doRecordMessage(
				messageId, 
				RESPONSE,
				null,
				this.messageMethod,
				responseTokens,
				millisecondsDouble,
				responseTime,
				engine.getEngineId(),
				insightId,
				sessionId,
				userId,
				userName
			);
		}
    }
}
