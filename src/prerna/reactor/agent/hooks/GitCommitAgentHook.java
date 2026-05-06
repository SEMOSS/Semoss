package prerna.reactor.agent.hooks;

import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.api.IEngine;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IMessageHook;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;


public final class GitCommitAgentHook implements IMessageHook {
	
	private static final Logger classLogger = LogManager.getLogger(GitCommitAgentHook.class);
	
    @Override
    public void afterMessage(AgentRunContext ctx, AgentHarnessResult result) throws Exception {
    	Map<String, Object> paramMap = ctx.getParamMap();
    	String projectId = Objects.toString(paramMap.get("project"), null);
    	if (projectId == null) {
    		classLogger.error("POST MESSAGE GIT COMMIT HOOK IS MISSING PROJECT ID");
    		return;
    	}
    	
    	IEngine projectEngine = Utility.getProject(projectId);
    	
    	String projectName = projectEngine.getEngineName();
    	
    	String gitFolder = EngineUtility.getSpecificEngineVersionFolder(CATALOG_TYPE.PROJECT, projectId, projectName);
    	
    	GitRepoUtils.addAllFiles(gitFolder, true);
    	
    	User user = ctx.getInsight().getUser();
    	
    	AccessToken accessToken = user.getAccessToken(user.getPrimaryLogin());
		String author = accessToken.getUsername();
		String email = accessToken.getEmail();
		GitRepoUtils.commitAddedFiles(gitFolder, "CODING AGENT EDIT", author, email);
    	
    }    	
}