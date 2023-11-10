package prerna.engine.impl.model.workers;

import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.job.JobReactor;
import prerna.util.Utility;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.ModelEngineConstants;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

public class ModelEngineInferenceLogsWorker implements Runnable {
	
	private String messageId;
	private String messageMethod;
    private AbstractModelEngine engine;
    private Insight insight;
    private String prompt;
    private String context;
    private LocalDateTime inputTime;
    private String response;
    private LocalDateTime responseTime;
    private Integer promptTokens;
    private Integer responseTokens;
    
    public ModelEngineInferenceLogsWorker(
		String messageId, 
		String messageMethod, 
		AbstractModelEngine engine,
		Insight insight, 
	   	String context,
	   	String prompt,
	   	Integer promptTokens,
	   	LocalDateTime inputTime,
	   	String response,
	   	Integer responseTokens,
	   	LocalDateTime responseTime
	) {
    	this.messageId = messageId;
    	this.messageMethod = messageMethod;
    	this.engine = engine;
    	this.insight = insight;
    	this.context = context;
        this.prompt = prompt;
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
		String userId = user.getPrimaryLoginToken().getId();
		// TODO this could be insight id
		
        Duration duration = Duration.between(inputTime, responseTime);
        long millisecondsDifference = duration.toMillis();
        Double millisecondsDouble = (double) millisecondsDifference;

        
		// TODO this needs to be moved to wherever we "publish" a new LLM/agent
		if (!ModelInferenceLogsUtils.doModelIsRegistered(engine.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(engine.getEngineId(), engine.getEngineName(), null, 
					engine.getModelType().toString(), user.getPrimaryLoginToken().getId());
		}
		
		if (!ModelInferenceLogsUtils.doCheckConversationExists(insightId)) {
			String roomName = null;
			if (Boolean.parseBoolean((String) engine.getSmssProp().get(ModelEngineConstants.GENERATE_ROOM_NAME)) == true) {
				// roomName = ModelInferenceLogsUtils.generateRoomTitle(engine, question);
				roomName = prompt.substring(0, Math.min(prompt.length(), 100));
			} else {
				roomName = prompt.substring(0, Math.min(prompt.length(), 100));
			}
			ModelInferenceLogsUtils.doCreateNewConversation(insightId, roomName, this.context, user.getPrimaryLoginToken().getId(), engine.getModelType().toString(), true, projectId, projectName, engine.getEngineId());
		}
				
		if(engine.keepInputOutput()) {
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					ModelEngineConstants.INPUT,
					prompt.replace("'", "\'").replace("\n", "\n"),
					this.messageMethod,
					promptTokens,
					millisecondsDouble,
					inputTime,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					ModelEngineConstants.RESPONSE,
					response.replace("'", "\'").replace("\n", "\n"),
					this.messageMethod,
					responseTokens,
					millisecondsDouble,
					responseTime,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
		} else {
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					ModelEngineConstants.INPUT,
					null,
					this.messageMethod,
					promptTokens,
					millisecondsDouble,
					inputTime,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					ModelEngineConstants.RESPONSE,
					null,
					this.messageMethod,
					responseTokens,
					millisecondsDouble,
					responseTime,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
		}
    }
}
