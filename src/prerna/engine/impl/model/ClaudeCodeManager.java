package prerna.engine.impl.model;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.CacheBuilder;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;
import prerna.engine.impl.model.responses.AskToolModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.InstructModelEngineResponse;
import prerna.engine.impl.model.workers.ModelEngineInferenceLogsWorker;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.util.DIHelper;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityUserAccessKeyUtils;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.util.EngineUtility;

public class ClaudeCodeManager {
	
	private static final Logger classLogger = LogManager.getLogger(ClaudeCodeManager.class);
	
	protected String prefix = null;
	protected String workingDirectory;
	protected String workingDirectoryBasePath = null;

	protected PyTranslator pyTranslator = null;
	protected File cacheFolder;
	private ClientProcessWrapper cpw = null;

	protected String varName = null;
	protected Map<String, String> vars = new HashMap<>();
	
	private String createInitScript(String engineId, String projectPath, String cliPath, String roomId) {
	    return String.format(
	        "import genai_client;claude_code = genai_client.ClaudeCodeClient(model='%s', cli_path='%s', cwd_path='%s', room_id='%s', access_key='%s', secret_key='%s')",
	        engineId,
	        cliPath,
	        projectPath,
	        roomId,
	        "790b8c5f-4817-4faf-b1d1-8647fe04e4be",
	        "d946f2fc-0f8b-4a7e-ae59-4e41d2e07cad"
	    );
	}
	
	private String createQueryScript(String prompt, String systemPrompt) {
		return String.format(
				"claude_code.query_cc(prompt='%s', system_prompt='%s')",
				prompt,
				systemPrompt
				);
		}
	
	
	public String query(Insight insight, User user, String engineId, String projectId, String prompt, String systemPrompt, String roomId) {
		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
		IModelEngine modelEngine = Utility.getModel(engineId);
		IProject project = Utility.getProject(projectId);
		if(project == null) {
			throw new IllegalArgumentException("Could not find or load project = " + projectId);
		}
		String projectName = project.getProjectName();
		String projectPath = EngineUtility.getSpecificEngineAssetsFolder(project.getCatalogType(), projectId, projectName);
		String claudeCodePath = DIHelper.getInstance().getCoreProp().getProperty("CLAUDE_CODE_PATH");
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, prompt);
		String finalRoomId = room.getId();
		String initScript = createInitScript(engineId, projectPath, claudeCodePath, finalRoomId);
		checkSocketStatus(initScript);
		String queryScript = createQueryScript(prompt, systemPrompt);
		Object output = pyTranslator.runDirectPy(insight, queryScript);
		return String.valueOf(output);
	}
	
	/**
	 * This method is responsible for starting the python process that is linked to
	 * this model engine.
	 * 
	 * @param port The port number to use when creating the server/client
	 *             connection.
	 */
	protected synchronized void startServer(int port, String initScript) {
		if (this.cpw != null && this.cpw.getSocketClient() != null && this.cpw.getSocketClient().isConnected()) {
			return;
		}
		if (this.workingDirectoryBasePath == null) {
			this.createCacheFolder();
		}

		// check if we have already created a process wrapper
		ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
		if (this.cpw != null) {
			this.cpw.shutdown(false);
		}

		String timeout = "30";

		if (cpwToInit.getSocketClient() == null) {
			boolean debug = false;

			// pull the relevant values from the smss
			String forcePort = null; // Not sure where I'd keep this; possibly as reactor param
			String customClassPath = null;
			String loggerLevel = null;

			if (port < 0) {
				// port has not been forced
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
						// ignore
						classLogger.warn("Claude Code" + " has an invalid FORCE_PORT value");
					}
				}
			}

			String serverDirectory = this.cacheFolder.getAbsolutePath();

			try {
				cpwToInit.createProcessAndClient(true, null, port, null, serverDirectory, customClassPath,
						debug, timeout, "INFO");
			} catch (Exception e) {
				classLogger.error("Failed to create the python process for Claude Code Agent: {}", e);
				throw new IllegalArgumentException("Unable to connect to server for python Claude Code Agent.");
			}
		} else if (!cpwToInit.getSocketClient().isConnected()) {
			cpwToInit.shutdown(false);
			try {
				cpwToInit.reconnect();
			} catch (Exception e) {
				classLogger.error("Failed to reconnect to the python process for Claude Code Agent: {}", e);
				throw new IllegalArgumentException("Failed to start TCP Server for Claude Code Agent: {}", e);
			}
		}

		// create the py translator
		Insight processInsight = new Insight();
		InsightStore.getInstance().put(processInsight);
		this.pyTranslator = new PyTranslator(cpwToInit.getSocketClient(), processInsight);

		try {
			// execute all the basic commands
			String initCommands = initScript;
			// break the commands seperated by ;
			String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
			// replace the Vars
			for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
				commands[commandIndex] = fillVars(commands[commandIndex]);
			}
			this.pyTranslator.runEmptyPy(commands);
			// for debugging...
			classLogger.info("Initializing Claude Code"
					+ " python process with commands >>> " + String.join("\n", commands));

			// run a prefix command
			setPrefix(cpwToInit);

			// finally set the cpw in the class
			this.cpw = cpwToInit;
		} catch (Exception e) {
			classLogger.error("Failed to  to the python process for Claude Code", e);
			if (cpwToInit != null) {
				classLogger.warn(
						"Able to start the python process for Claude Code but the start script failed");
				cpwToInit.shutdown(false);
			}
			throw e;
		}
	}
	
	/**
	 * This method checks whether the socket client is instantiated and connected.
	 */
	protected void checkSocketStatus(String initScript) {
		if (this.cpw == null || this.cpw.getSocketClient() == null || !this.cpw.getSocketClient().isConnected()) {
			this.startServer(-1, initScript);
		}
	}
	
	/**
	 * 
	 */
	private void setPrefix(ClientProcessWrapper cpwToInit) {
		this.prefix = cpwToInit.getPrefix();
		PayloadStruct prefixPayload = new PayloadStruct();
		prefixPayload.payload = new String[] { "prefix", this.prefix };
		prefixPayload.operation = PayloadStruct.OPERATION.CMD;
		cpwToInit.getSocketClient().executeCommand(prefixPayload);
	}
	
	/**
	 * 
	 */
	private void createCacheFolder() {
		this.workingDirectory = "CLAUDECODE_" + "_" + Utility.getRandomString(6);
		this.workingDirectoryBasePath = Utility.getInsightCacheDir() + "/" + this.workingDirectory;
		this.cacheFolder = new File(workingDirectoryBasePath);

		// make the folder if one does not exist
		if (!this.cacheFolder.exists()) {
			this.cacheFolder.mkdir();
		}
	}
	
	/**
	 * 
	 * @param input
	 * @return
	 */
	private String fillVars(String input) {
		StringSubstitutor sub = new StringSubstitutor(vars);
		String resolvedString = sub.replace(input);
		return resolvedString;
	}


}
