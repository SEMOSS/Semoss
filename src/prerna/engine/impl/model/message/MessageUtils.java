package prerna.engine.impl.model.message;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.date.SemossDate;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.util.Utility;
import prerna.util.gson.SemossDateAdapter;

public class MessageUtils {

	private static Logger classLogger = LogManager.getLogger(MessageUtils.class);
	
	private static final Pattern MARKDOWN_CODE_PATTERN = Pattern.compile("```" + // Opening backticks
			"(?:([a-zA-Z0-9]+))?" + // Language (optional, group 1)
			"(?:" + // Non-capturing group for title alternatives
			"\\s+title=\"([^\"]+)\"" + // Either title="filename" (group 2)
			"|\\s+([^\\s\\n]+)" + // Or direct filename (group 3)
			")?" + // Title is optional
			"\\s*\\n" + // Whitespace and mandatory newline
			"(.*?)" + // Code content (group 4)
			"```", // Closing backticks
			Pattern.DOTALL);

	private static final ExclusionStrategy NO_ROOM_INSIGHT_SOCKET_EXCLUSION = new ExclusionStrategy() {
		@Override
		public boolean shouldSkipField(FieldAttributes f) {
			String fieldName = f.getName();
			if ("room".equals(fieldName) || "insight".equals(fieldName))
				return true;
			Type declaredType = f.getDeclaredType();
			if (declaredType instanceof Class<?>) {
				Class<?> declaredClass = (Class<?>) declaredType;
				if (Room.class.isAssignableFrom(declaredClass) || Insight.class.isAssignableFrom(declaredClass)
						|| Socket.class.isAssignableFrom(declaredClass))
					return true;
			}
			return false;
		}

		@Override
		public boolean shouldSkipClass(Class<?> clazz) {
			return Room.class.isAssignableFrom(clazz) || Insight.class.isAssignableFrom(clazz)
					|| Socket.class.isAssignableFrom(clazz);
		}
	};

	// For DB: skips "room", "insight", "socket", and "base64Data"
	private static final Gson GSON_FOR_DB = new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.addSerializationExclusionStrategy(NO_ROOM_INSIGHT_SOCKET_EXCLUSION)
			.addSerializationExclusionStrategy(new ExclusionStrategy() {
				@Override
				public boolean shouldSkipField(FieldAttributes f) {
					return "base64Data".equals(f.getName());
				}

				@Override
				public boolean shouldSkipClass(Class<?> clazz) {
					return false;
				}
			}).create();

	// For Python: skips "room", "insight", "socket", "paramMap", includes
	// base64Data
	private static final Gson GSON_FOR_PY = new GsonBuilder()
			.disableHtmlEscaping()
			.registerTypeAdapter(SemossDate.class, new SemossDateAdapter())
			.addSerializationExclusionStrategy(NO_ROOM_INSIGHT_SOCKET_EXCLUSION)
			.addSerializationExclusionStrategy(new ExclusionStrategy() {
				@Override
				public boolean shouldSkipField(FieldAttributes f) {
					return "paramMap".equals(f.getName());
				}

				@Override
				public boolean shouldSkipClass(Class<?> clazz) {
					return false;
				}
			}).create();

	// ---- Serialization/Deserialization ----

	// Deserialize a single message from JSON
	public static AbstractMessage fromJson(String json, Room room) {
		JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();
		MessageType type = MessageType.valueOf(jsonObj.get("type").getAsString());
		AbstractMessage message = null;
		switch (type) {
			case RESPONSE_TEXT:
			case RESPONSE_TOOL:
				message = GSON_FOR_DB.fromJson(json, ResponseMessage.class);
				break;
			case INPUT_MEDIA:
				message = GSON_FOR_DB.fromJson(json, InputMessage.class);
				// re-encode the base64 from file.
				for (ImageInfo imageInfo : ((InputMessage) message).getImageInfos()) {
					imageInfo.setRoomFolder(room.getRoomFolderPath());
					imageInfo.getBase64Data();
				}
				break;
			case INPUT_TEXT:
				message = GSON_FOR_DB.fromJson(json, InputMessage.class);
				break;
			default:
				classLogger.warn("Unhandled fromJSON for message type = " + type);
		}
		if (message != null) {
			message.setRoom(room);
		}
		return message;
	}

	// Serialize any message to JSON (for DB)
	public static String toJson(AbstractMessage msg) {
		return GSON_FOR_DB.toJson(msg);
	}

	// Deserialize from JSON array string to List<AbstractMessage>
	public static List<AbstractMessage> fromJsonArray(String jsonArrayString, Room room) {
		if (jsonArrayString == null || jsonArrayString.trim().isEmpty()) {
			return new ArrayList<>();
		}
		JsonArray array = JsonParser.parseString(jsonArrayString).getAsJsonArray();
		List<AbstractMessage> result = new ArrayList<>();
		for (JsonElement elem : array) {
			AbstractMessage message = fromJson(elem.toString(), room);
			if (message != null) {
				result.add(message);
			}
		}
		return result;
	}

	// --- Core two serialization methods ---

	// For DB: JSON array string of messages, with NO base64
	public static String toJsonArray(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}
		return GSON_FOR_DB.toJson(msgs);
	}

	
	public static String getMessageHistoryFromMessageId(List<AbstractMessage> messages, String latestMessageId) {
		return toJsonArrayWithImageData(getMessageBranch(messages, latestMessageId));
	}
	
	
	// For Python: JSON array string WITH base64 image data in ImageInfo
	public static String toJsonArrayWithImageData(List<AbstractMessage> msgs) {
		if (msgs == null || msgs.isEmpty()) {
			return "[]";
		}
		// Ensure base64Data is loaded for all images
		for (AbstractMessage msg : msgs) {
			if (msg instanceof InputMessage) {
				InputMessage input = (InputMessage) msg;
				if (input.hasImages()) {
					for (ImageInfo img : input.getImageInfos()) {
						// Populate the field (it will actually load the file if needed)
						img.setBase64Data(img.getBase64Data());
					}
				}
			}
		}
		return GSON_FOR_PY.toJson(msgs);
	}
	
	public static List<AbstractMessage> getMessageBranch(List<AbstractMessage> messages, String latestMessageId) {
	    // 1. Build lookup map (messageId to message)
	    Map<String, AbstractMessage> idMap = new HashMap<>();
	    for (AbstractMessage m : messages) {
	        if (m.getMessageId() != null) {
	            idMap.put(m.getMessageId(), m);
	        }
	    }
	    // 2. Climb up parent chain
	    List<AbstractMessage> history = new ArrayList<>();
	    String currentId = latestMessageId;
	    while (currentId != null) {
	        AbstractMessage m = idMap.get(currentId);
	        if (m == null) break;
	        history.add(m);
	        // parentMessageId may be null/empty String
	        currentId = m.getParentMessageId();
	        if (currentId == null || currentId.isEmpty()) break;
	    }
	    // 3. Messages are from newest-to-oldest; reverse to get root-to-leaf
	    Collections.reverse(history);
	    return history;
	}
	
	public static List<AbstractMessage> convertFullPrompt(Object fullPrompt, Room room, IModelEngine modelEngine) {
	    List<AbstractMessage> result = new ArrayList<>();
	    List<?> promptList;

	    if (fullPrompt instanceof String) {
	        promptList = new Gson().fromJson((String) fullPrompt, List.class);
	    } else if (fullPrompt instanceof List<?>) {
	        promptList = (List<?>) fullPrompt;
	    } else {
	        throw new IllegalArgumentException("fullPrompt must be a JSON string or List<Map>.");
	    }

	    for (Object o : promptList) {
	        if (!(o instanceof Map)) continue;
	        Map<?, ?> map = (Map<?, ?>) o;
	        String role = asStringOrNull(map.get("role"));
	        String content = asStringOrNull(map.get("content"));

	        // -------- SYSTEM --------
	        if ("system".equals(role)) {
	            // This sets system prompt/context in the room - don't append as message
	            room.setSystemMessage(content);
	            continue;
	        }

	        // -------- USER (TEXT and/or IMAGE) --------
	        if ("user".equals(role)) {
	            List<String> imageList = new ArrayList<>();
	            String textPart = "";
	            // OpenAI-style: content is a list of dicts with type text/image_url
	            Object contentObj = map.get("content");
	            if (contentObj instanceof List<?>) {
	                for (Object part : (List<?>) contentObj) {
	                    if (!(part instanceof Map)) continue;
	                    Map<?,?> partMap = (Map<?,?>)part;
	                    String type = asStringOrNull(partMap.get("type"));
	                    if ("text".equals(type)) {
	                        textPart += asStringOrNull(partMap.get("text"));
	                    } else if ("image_url".equals(type)) {
	                        // e.g. { "type": "image_url", "image_url": { "url": ... } }
	                        Object imgURLObj = partMap.get("image_url");
	                        if (imgURLObj instanceof Map) {
	                            String url = asStringOrNull(((Map<?,?>)imgURLObj).get("url"));
	                            if (url != null) imageList.add(url);
	                        }
	                    }
	                }
	            } else if (contentObj instanceof String) {
	                textPart = (String) contentObj;
	            }

	            InputMessage.Builder builder = InputMessage.builder(room)
	                .withInputUIPrompt(textPart)
	                .withInputPrompt(textPart)
	                .withModelType(modelEngine.getModelType());

	            if (!imageList.isEmpty()) builder.withImageUrls(imageList);

	            // If you receive extra tools for this turn:
	            Object toolsObj = map.get("tools");
	            if (toolsObj instanceof List<?>) {
	                builder.withTools((List<Map<String,Object>>)toolsObj);
	            }

	            result.add(builder.build());
	            continue;
	        }

	        // -------- ASSISTANT --------
	        // -------- ASSISTANT --------
	        if ("assistant".equals(role)) {
	            Object toolCallsObj = map.get("tool_calls");

	            // -- If assistant provides tool_calls, flatten as tool_responses
	            if (toolCallsObj instanceof List<?> && !((List<?>)toolCallsObj).isEmpty()) {
	                List<Map<String,Object>> flattenedTools = new ArrayList<>();
	                for (Object elem : (List<?>) toolCallsObj) {
	                    if (elem instanceof Map) {
	                        Map<?,?> callMap = (Map<?,?>)elem;
	                        Map<String,Object> flatTool = new HashMap<>();
	                        flatTool.put("id", asStringOrNull(callMap.get("id")));   // tool_call id
	                        flatTool.put("type", asStringOrNull(callMap.get("type")));
	                        // openAI: "function": {...}
	                        Object functionObj = callMap.get("function");
	                        if ("function".equals(flatTool.get("type")) && functionObj instanceof Map) {
	                            Map<?,?> funcMap = (Map<?,?>) functionObj;
	                            flatTool.put("name", asStringOrNull(funcMap.get("name")));
	                            flatTool.put("arguments", asStringOrNull(funcMap.get("arguments"))); // stringified JSON
	                        } else {
	                            // For non-function tools, flatten as key-values
	                            for (Map.Entry<?,?> entry : callMap.entrySet()) {
	                                if (!"id".equals(entry.getKey()) && !"type".equals(entry.getKey()))
	                                    flatTool.put(String.valueOf(entry.getKey()), entry.getValue());
	                            }
	                        }
	                        flattenedTools.add(flatTool);
	                    }
	                }
	                ResponseMessage.Builder builder = ResponseMessage.builder();
	                builder.withType(MessageType.RESPONSE_TOOL); // This marks as RESPONSE_TOOL
	                builder.withText(asStringOrNull(content)); // Preserves the content/text if present
	                builder.withToolResponses(flattenedTools);
	                result.add(builder.build());
	                continue;
	            }
	            // -- Otherwise: classic assistant response
	            ResponseMessage.Builder builder = ResponseMessage.builder();
	            builder.withText(asStringOrNull(content));
	            result.add(builder.build());
	            continue;
	        }

	        // -------- TOOL/FUNCTION CALL (user-provided tools executed) --------
	        if ("function".equals(role) || "tool".equals(role)) {
	            String toolName = asStringOrNull(map.get("name"));
	            String toolResult = asStringOrNull(map.get("content"));
	            String toolCallId = asStringOrNull(map.get("tool_call_id")); 

	            // Add as tool execution message (in my earlier pattern)
	            AbstractMessage toolExecMsg = InputMessage.toolExecution(room, toolCallId, toolName, toolResult, null);
	            result.add(toolExecMsg);
	            continue;
	        }

	    }
	    return result;
	}

	
	// Utility: to get string or return null if not a string
	private static String asStringOrNull(Object o) {
	    return (o instanceof String) ? (String) o : null;
	}

	// ---- Utility/Convenience methods (maintain if needed) ----

	// These can alias to above or be retained for backwards compatibility
	public static String getMessagesForDatabase(List<AbstractMessage> msgs) {
		return toJsonArray(msgs);
	}

	public static String getMessagesForPy(List<AbstractMessage> msgs) {
		return toJsonArrayWithImageData(msgs);
	}

	// ---- Image move utilities ---- This should be used over copy

	public static List<String> moveFilesToRoomFolder(List<String> relativePathToFiles, Room room, Insight insight) {
		List<String> roomFilePaths = new ArrayList<>();
		if (relativePathToFiles == null || relativePathToFiles.isEmpty()) {
			classLogger.info("No file paths provided to move.");
			return roomFilePaths;
		}
		String insightFolder = insight.getInsightFolder(); // absolute path to insight folder
		String roomFolder = room.getRoomFolderPath(); // absolute path to room folder
		Path targetDir = Paths.get(roomFolder);
		try {
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			classLogger.warn("Failed to create room folder: " + targetDir, e);
			return roomFilePaths;
		}
		for (String relPath : relativePathToFiles) {
			File srcFile = new File(insightFolder, relPath);
			if (!srcFile.exists() || !srcFile.isFile()) {
				classLogger.info("Source file does not exist in insight folder: " + srcFile.getAbsolutePath());
				continue;
			}
			String fileName = srcFile.getName();
			Path destination = targetDir.resolve(fileName);
			try {
				Files.move(srcFile.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				classLogger.warn("Failed to move file: " + srcFile.getAbsolutePath() + " to " + destination, e);
				continue;
			}
			roomFilePaths.add(destination.toString());
		}
		return roomFilePaths;
	}

	// ---- Image copy utilities ----
	public static List<String> copyFilesToRoomFolder(List<String> relativePathToFiles, Room room, Insight insight) {
		List<String> copiedFileNames = new ArrayList<>();
		if (relativePathToFiles == null || relativePathToFiles.isEmpty()) {
			classLogger.info("No file paths provided to copy.");
			return copiedFileNames;
		}
		String insightFolder = insight.getInsightFolder(); // absolute path to insight folder
		String roomFolder = room.getRoomFolderPath(); // absolute path to room folder
		Path targetDir = Paths.get(roomFolder);
		try {
			Files.createDirectories(targetDir);
		} catch (IOException e) {
			classLogger.warn("Failed to create room folder: " + targetDir, e);
			return copiedFileNames;
		}
		for (String relPath : relativePathToFiles) {
			File srcFile = new File(insightFolder, relPath);
			if (!srcFile.exists() || !srcFile.isFile()) {
				classLogger.info("Source file does not exist in insight folder: " + srcFile.getAbsolutePath());
				continue;
			}
			String fileName = srcFile.getName();
			Path destination = targetDir.resolve(fileName);
			try {
				Files.copy(srcFile.toPath(), destination, StandardCopyOption.REPLACE_EXISTING);
				copiedFileNames.add(fileName); // only add if copy succeeded
			} catch (IOException e) {
				classLogger.warn("Failed to copy file: " + srcFile.getAbsolutePath() + " to " + destination, e);
			}
		}
		return copiedFileNames;
	}

	// Method to parse markdown code blocks
	public static ResponseMessage processMarkdownCodeBlocks(ResponseMessage responseMessage, IModelEngine modelEngine,
			Room room) {
		String rawResponse = responseMessage.getContent();

		Map<String, CodeBlock> codeBlocks = new HashMap<>();
		Matcher matcher = MARKDOWN_CODE_PATTERN.matcher(rawResponse);
		StringBuffer modifiedResponse = new StringBuffer();

		while (matcher.find()) {
			String language = matcher.group(1) != null ? matcher.group(1).trim() : "";
			// Check both title formats and use the first non-null one
			String title = matcher.group(2) != null ? matcher.group(2).trim()
					: matcher.group(3) != null ? matcher.group(3).trim() : "";
			String code = matcher.group(4).trim();

			String uuid = UUID.randomUUID().toString();

			if (title == "") {
				HashMap<String, Object> paramMap = new HashMap<String, Object>();
				paramMap.put("use_history", "false");
				InputMessage msg = InputMessage.builder(room)
						.withInputUIPrompt(
								"Given the following code block, give it a title: " + code + " Just give me the title")
						.withInputPrompt(
								"Given the following code block, give it a title: " + code + " Just give me the title")
						.withModelType(modelEngine.getModelType()).withParamMap(paramMap).build();

				ResponseMessage response = room.ask(msg, null, modelEngine);
				title = response.getContent();
			}

			codeBlocks.put(uuid, new CodeBlock(language, code, title));

			matcher.appendReplacement(modifiedResponse,
					Matcher.quoteReplacement("<CODEBLOCK>" + uuid + "</CODEBLOCK>"));
		}
		matcher.appendTail(modifiedResponse);

		responseMessage.setOrnament("processedResponsed", modifiedResponse.toString());
		responseMessage.setOrnament("codeBlocks", codeBlocks);

		return responseMessage;
	}

	// Class to represent a code block
	private static class CodeBlock {
		private final String language;
		private final String code;
		private final String title;

		public CodeBlock(String language, String code, String title) {
			this.language = language;
			this.code = code;
			this.title = title;
		}

		public String getLanguage() {
			return language;
		}

		public String getCode() {
			return code;
		}

		public String getTitle() {
			return title;
		}
	}
	
	// ---- Tool Response utilities ---- 

	/**
	 * 
	 * @param response
	 */
	public static void updateToolResponseWithProjectMeta(ResponseMessage response) {
		Map<String, JSONObject> mcpToolsJsonCache = new HashMap<>();
		List<Map<String, Object>> toolResponses = response.getToolResponses();
		for(int toolResponseIndex = 0; toolResponseIndex < toolResponses.size(); toolResponseIndex++) {
			Map<String, Object> responseToolMap = toolResponses.get(toolResponseIndex);
			// we start the function name with _projectid_ so lets remove that
			String responseProjectIdToolFunctionName = (String) responseToolMap.get("name");
			if(!responseProjectIdToolFunctionName.startsWith("_")) {
				// if the tool function doesn't start with _projectid_
				// then this response is already in proper format for the FE
				continue;
			}
			String[] responseProjectIdToolFunctionNameSplit = responseProjectIdToolFunctionName.substring(1).split("_", 2);
			String projectId = responseProjectIdToolFunctionNameSplit[0];
			String origFunctionName = responseProjectIdToolFunctionNameSplit[1];
			
			// now that we have the projectId
			// lets append some of the mcp metadata back into the response
			
			JSONObject mcpToolsJson = mcpToolsJsonCache.get(projectId);
			if(mcpToolsJson == null) {
				IProject project = Utility.getProject(projectId);
				if(project == null) {
					// technically speaking you could have a function start with _
					// but will assume this is in proper format
					continue;
				}
				mcpToolsJson = MCPUtility.getAggregatedTools(project);
				mcpToolsJsonCache.put(projectId, mcpToolsJson);
			}
			
			if(mcpToolsJson != null) {
				JSONArray mcpToolsArray = mcpToolsJson.getJSONArray("tools");
				JSONObject mcpTool = null;
				PROJECT_MCP_LOOP : for(int toolIndex = 0; toolIndex < mcpToolsArray.length(); toolIndex++) {
					JSONObject _tool = mcpToolsArray.getJSONObject(toolIndex);
					if(_tool.has("name") && _tool.getString("name").equals(origFunctionName)) {
						mcpTool = _tool;
						break PROJECT_MCP_LOOP;
					}
				}
				
				// add back the title from mcp structure
				if(mcpTool != null && mcpTool.has("title")) {
					responseToolMap.put("title", mcpTool.getString("title"));
				}
				
				if(mcpToolsJson.has("_meta")) {
					responseToolMap.put("_meta", mcpToolsJson.get("_meta"));
				}
			}
			
			// now update the json name to be the original tool name
			responseToolMap.put("name", origFunctionName);
		}
	}
}