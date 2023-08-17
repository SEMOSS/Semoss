package prerna.engine.impl.model.workers;

import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.util.Utility;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

public class ModelEngineInferenceLogsWorker implements Runnable {
	
	private static final String MESSAGE_CONTENT = "content";
	private static final String ROLE = "role";
	
	private String roomId;
	private String messageId;
	private String messageMethod;
    private AbstractModelEngine engine;
    private Insight insight;
    private String question;
    private LocalDateTime inputTime;
    private String response;
    private LocalDateTime responseTime;
    
    public ModelEngineInferenceLogsWorker(String roomId, String messageId, String messageMethod, AbstractModelEngine engine,
			   Insight insight, 
			   String question,
			   LocalDateTime inputTime,
			   String response,
			   LocalDateTime responseTime) {
    	this.roomId = roomId;
    	this.messageId = messageId;
    	this.messageMethod = messageMethod;
    	this.engine = engine;
    	this.insight = insight;
        this.question = question;
        this.inputTime = inputTime;
        this.response = response;
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
		
		
		// TODO this needs to be moved to wherever we "publish" a new LLM/agent
		if (!ModelInferenceLogsUtils.doModelIsRegistered(engine.getEngineId())) {
			ModelInferenceLogsUtils.doCreateNewAgent(engine.getEngineId(), engine.getEngineName(), null, 
					engine.getModelType().toString(), user.getPrimaryLoginToken().getId());
		}
		
		if (!ModelInferenceLogsUtils.doCheckConversationExists(roomId)) {
			String roomName = null;
			if (Boolean.parseBoolean((String) engine.getSmssProp().get("GENERATE_ROOM_NAME")) == true) {
				roomName = ModelInferenceLogsUtils.generateRoomTitle(engine, question);
			}
			ModelInferenceLogsUtils.doCreateNewConversation(roomId, roomName, "", "{}", user.getPrimaryLoginToken().getId(), engine.getModelType().toString(), true, projectId, projectName, engine.getEngineId());
		}
				
		if(engine.keepsConversationHistory()) {
			Map<String, Object> inputOutputMap = new HashMap<String, Object>();
			inputOutputMap.put(ROLE, "user");
			inputOutputMap.put(MESSAGE_CONTENT, question);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					"INPUT",
					question.replace("'", "\'").replace("\n", "\n"),
					this.messageMethod,
					ModelInferenceLogsUtils.getTokenSizeString(question),
					inputTime,
					roomId,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
			inputOutputMap.put(ROLE, "assistant");
			inputOutputMap.put(MESSAGE_CONTENT, response);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					"RESPONSE",
					response.replace("'", "\'").replace("\n", "\n"),
					this.messageMethod,
					ModelInferenceLogsUtils.getTokenSizeString(response),
					responseTime,
					roomId,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
		} else {
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					"INPUT",
					null,
					this.messageMethod,
					ModelInferenceLogsUtils.getTokenSizeString(question),
					inputTime,
					roomId,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
			ModelInferenceLogsUtils.doRecordMessage(messageId, 
					"RESPONSE",
					null,
					this.messageMethod,
					ModelInferenceLogsUtils.getTokenSizeString(response),
					responseTime,
					roomId,
					engine.getEngineId(),
					insightId,
					sessionId,
					userId
					);
		}
    }
}
