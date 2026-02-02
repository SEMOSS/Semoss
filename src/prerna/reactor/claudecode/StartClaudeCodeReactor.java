package prerna.reactor.claudecode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.database.CommandReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class StartClaudeCodeReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(StartClaudeCodeReactor.class);
	
	public StartClaudeCodeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey() };
		this.keyRequired = new int[] { 1 };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(ReactorKeysEnum.PROJECT.getKey());
		User user = this.insight.getUser();
		
		boolean projectContext = this.insight.setContext(projectId);
		if (!projectContext) {
			throw new RuntimeException("Failed to set project context");
		}
		
		CmdExecUtil cmdUtil = this.insight.getCmdUtil();
		
		// Is it on the PATH?
		String whichResult = cmdUtil.executeCommand("which claude");
		classLogger.info("which claude: " + whichResult);

		// Can it respond at all?
		String versionResult = cmdUtil.executeCommand("claude --version");
		classLogger.info("claude --version: " + versionResult);

		// What does the environment look like?
		String envResult = cmdUtil.executeCommand("env");
		classLogger.info("environment: " + envResult);
		
		String response = cmdUtil.executeCommand("claude");
		classLogger.info("Claude command response: " + response);

		return new NounMetadata(response, PixelDataType.CONST_STRING);
	}

}
