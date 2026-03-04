package prerna.engine.impl.model;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.ThreadStore;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.IModelEngine;
import prerna.om.ClientProcessWrapper;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.tcp.PayloadStruct;
import prerna.util.Utility;
import prerna.project.api.IProject;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.util.EngineUtility;
import java.util.stream.Collectors;

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
	
	private String createInitScript(String engineId, String projectPath, String roomId, String accessKey, String secretKey, List<String> allowedTools, String permissionMode) {
		String allowedToolsString = "allowed_tools=[" + allowedTools.stream()
	    .map(tool -> "'" + tool + "'")
	    .collect(Collectors.joining(",")) + "]";
		Integer localPort = ThreadStore.getLocalPort();
		String localHostname = ThreadStore.getLocalHostname();
    	String localProtocol = ThreadStore.getLocalProtocol();
        	String baseUrl = localProtocol + "://" + localHostname + ":" + localPort + "/Monolith/api/model/anthropic";
	    return String.format(
	        "import genai_client;claude_code = genai_client.ClaudeCodeClient(model='%s', cwd_path='%s', room_id='%s', access_key='%s', secret_key='%s', %s, permission_mode='%s', base_url='%s')",
	        engineId,
	        projectPath,
	        roomId,
	        accessKey,
	        secretKey,
	        allowedToolsString,
	        permissionMode,
	        baseUrl
	    );
	}
	
	private String createQueryScript(String prompt, String systemPrompt) {
		return String.format(
				"claude_code.query_cc(prompt='%s', system_prompt='%s')",
				prompt,
				systemPrompt
				);
		}
	
	
	public String query(Insight insight, User user, String engineId, String projectId, String prompt, String systemPrompt, String roomId, List<String> allowedTools, String permissionMode) {
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
		Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, prompt);
		String finalRoomId = room.getId();
		String[] keyPair = user.createCachedTemporalAccessSecretKey();
		String accessKey = keyPair[0];
		String secretKey = keyPair[1];
		String initScript = createInitScript(engineId, projectPath, finalRoomId, accessKey, secretKey, allowedTools, permissionMode);
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

		ClientProcessWrapper cpwToInit = new ClientProcessWrapper();
		if (this.cpw != null) {
			this.cpw.shutdown(false);
		}

		String timeout = "30";

		if (cpwToInit.getSocketClient() == null) {
			boolean debug = false;

			String forcePort = null; // Not sure where I'd keep this; possibly as reactor param
			String customClassPath = null;
			String loggerLevel = null;

			if (port < 0) {
				if (forcePort != null && !(forcePort = forcePort.trim()).isEmpty()) {
					try {
						port = Integer.parseInt(forcePort);
						debug = true;
					} catch (NumberFormatException e) {
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
			String initCommands = initScript;
			String[] commands = initCommands.split(PyUtils.PY_COMMAND_SEPARATOR);
			for (int commandIndex = 0; commandIndex < commands.length; commandIndex++) {
				commands[commandIndex] = fillVars(commands[commandIndex]);
			}
			this.pyTranslator.runEmptyPy(commands);
			classLogger.info("Initializing Claude Code"
					+ " python process with commands >>> " + String.join("\n", commands));
			setPrefix(cpwToInit);

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
