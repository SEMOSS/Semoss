package prerna.reactor.export;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/**
 * Simple reactor that generates HTML content using LLM based on a question.
 * Returns a map with the
 * filled HTML template and the generated title/body content.
 */
public class GenerateHtmlContentReactor extends AbstractReactor {

  private static final String QUESTION_KEY = "question";
  private static final String LOGO_URL_KEY = "logoUrl";
  private static final String ENGINE_KEY = "engine";
  private static final Gson GSON = new GsonBuilder().create();

  public GenerateHtmlContentReactor() {
    this.keysToGet = new String[] {
        QUESTION_KEY,
        ENGINE_KEY,
        LOGO_URL_KEY,
        ReactorKeysEnum.ROOM_ID.getKey(),
        ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
    };
    this.keyRequired = new int[] { 1, 1, 0, 0, 0 }; // question and engine required, logoUrl, room, paramMap optional
  }

  @Override
   public NounMetadata execute() {
    try {
      String question = this.keyValue.get(QUESTION_KEY);
      String logoUrl = this.keyValue.get(LOGO_URL_KEY);
      String engineId = this.keyValue.get(ENGINE_KEY);
      String roomId = this.keyValue.get(ReactorKeysEnum.ROOM_ID.getKey());

      if (question == null || question.trim().isEmpty()) {
        throw new IllegalArgumentException("Question is required");
      }

      if (engineId == null || engineId.trim().isEmpty()) {
        throw new IllegalArgumentException("Engine is required");
      }

      User user = this.insight.getUser();
      if (user == null) {
        throw new IllegalArgumentException("User not properly logged in");
      }

      if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
        throw new IllegalArgumentException(
            "Model " + engineId + " does not exist or user does not have access to this model");
      }

      if (roomId == null) {
        roomId = UUID.randomUUID().toString();
      }

      IModelEngine modelEngine = Utility.getModel(engineId);

      // Set default logo if not provided
      if (logoUrl == null || logoUrl.trim().isEmpty()) {
        // Use a safe placeholder to avoid URL encoding issues
        logoUrl = ""; // Empty string - we'll handle this in template replacement
      }

      // Load template
      String template = loadTemplate();

      // Generate title and body using LLM
      Map<String, String> llmContent = generateContentWithLLM(question, modelEngine, roomId);
      String title = llmContent.get("title");
      String bodyContent = llmContent.get("body");

      // Fill template
      String filledHtml = template
          .replace("{{TITLE}}", escapeHtml(title))
          .replace(
              "{{DATE}}", LocalDate.now().format(DateTimeFormatter.ofPattern("MMMM d, yyyy")))
          .replace("{{BODY_CONTENT}}", bodyContent);

      // Handle logo replacement - remove img tag if no logo provided
      if (logoUrl == null || logoUrl.trim().isEmpty()) {
        // Simple string replacement to remove the logo img tag - match the exact
        // template format
        String logoImgTag = "<img src=\"{{LOGO_URL}}\" alt=\"Logo\" style=\"width: 100px; height: auto;"
            + " margin-bottom: 15px;\" />";
        filledHtml = filledHtml.replace(logoImgTag, "");
      } else {
        filledHtml = filledHtml.replace("{{LOGO_URL}}", escapeHtml(logoUrl));
      }

      // Aggressive cleanup to prevent URL encoding issues
      filledHtml = finalHtmlCleanup(filledHtml);
      filledHtml = sanitizeForUrlPassing(filledHtml);

      // Create HTML file in insight root folder
      String fileName = createHtmlFile(filledHtml, title);

      // Return simple map
      Map<String, Object> result = new HashMap<>();
      result.put("fileName", fileName);
      result.put("TITLE", title);
      result.put("BODY_CONTENT", bodyContent);

      return new NounMetadata(fileName, PixelDataType.CONST_STRING);

    } catch (IOException e) {
      throw new SemossPixelException(
          "Error reading template: " + e.getMessage(), e);
    } catch (Exception e) {
      throw new SemossPixelException(
          "Error generating HTML content: " + e.getMessage(), e);
    }
  }

  /** Generate title using LLM. */
  private Map<String, String> generateContentWithLLM(
      String question, IModelEngine modelEngine, String roomId) {
    String title = null;
    String body = null;
    try {
      Room room = RoomUtils.createRoomIfNotExists(roomId, insight, modelEngine, question);

      String titlePrompt = "Generate a concise, professional title for a document that answers this question: "
          + question
          + ". Return only the title, no additional text.";

      String bodyPrompt = "Write a comprehensive, professional document body that answers this question: "
          + question
          + ". Format the response using ONLY these HTML tags with inline styles: <h2"
          + " style=\"color: black; font-size: 14pt; margin-top: 20px; margin-bottom: 10px;\">,"
          + " <h3 style=\"color: black; font-size: 12pt; margin-top: 15px; margin-bottom:"
          + " 8px;\">, <h4 style=\"color: black; font-size: 12pt; margin-top: 10px;"
          + " margin-bottom: 5px;\">, <p style=\"margin-bottom: 12px;\">, <ul"
          + " style=\"margin-bottom: 15px; padding-left: 25px;\">, <ol style=\"margin-bottom:"
          + " 15px; padding-left: 25px;\">, <li style=\"margin-bottom: 5px;\">, <strong"
          + " style=\"font-weight: bold;\">, <em style=\"font-style: italic;\">. Structure the"
          + " content with clear sections like Overview, Key Points, Analysis, and Conclusion."
          + " Keep paragraphs concise and well-organized. Always include the inline styles as"
          + " shown above.";

      String prompt = "Generate a map with title and body where they are filled in as follows: \n\n";

      prompt += "Title: \n" + titlePrompt + "\n\n";
      prompt += "Body: \n" + bodyPrompt + "\n\n";
      prompt += "The final output should look like: \n\n"
          + " {title: sample title, body: sample body} \n\n"
          + " Do not wrap in code blocks";

      Map<String, Object> paramMap = getParamMap();
      if (paramMap == null) {
        paramMap = new HashMap<String, Object>();
      }

      // add response format to ensure json schema
      paramMap.put("schema", getJsonSchema());

      InputMessage msg = InputMessage.builder(room)
          .withInputPrompt(prompt)
          .withModelType(modelEngine.getModelType())
          .withParamMap(paramMap)
          .build();

      ResponseMessage response = room.ask(msg, modelEngine);
      String responseString = response.getContent();

      Map<String, Object> responseMap = parseResponse(responseString.trim());
      if (responseMap == null
          || !responseMap.containsKey("title")
          || !responseMap.containsKey("body")) {
        throw new SemossPixelException("LLM could not generate proper response");
      }

      // Get the response content and clean it up
      title = (String) responseMap.get("title");
      if (title != null && !title.trim().isEmpty()) {
        // Clean up extra whitespace, newlines, and problematic quotes
        title = title
            .trim()
            .replace("\\n", " ") // Convert literal \n to spaces
            .replace("\\r", " ") // Convert literal \r to spaces
            .replaceAll("\\s+", " "); // Collapse multiple spaces
      }
      body = (String) responseMap.get("body");
      if (body != null && !body.trim().isEmpty()) {
        // Light cleanup - preserve HTML structure but clean problematic characters
        body = body.trim()
            .replace("\\n", "\n") // Convert literal \n to actual newlines
            .replace("\\r", "\r") // Convert literal \r to actual carriage returns
            .replaceAll("\\n\\s*\\n\\s*\\n+", "\n\n") // Remove excessive blank lines
            .replaceAll("[''`]", ""); // Remove apostrophes, single quotes, and backticks
      }
    } catch (Exception e) {
      throw new SemossPixelException("Error generating title: " + e.getMessage(), e);
    }
    Map<String, String> llmContent = new HashMap<>();
    llmContent.put("title", title);
    llmContent.put("body", body);

    return llmContent;
  }

  /** Create an HTML file in the insight root folder and return the filename. */
  private String createHtmlFile(String htmlContent, String title) throws IOException {
    // Get the insight root folder - use the project assets folder as fallback
    String rootFolder = this.insight.getInsightFolder();

    // Create a safe filename based on title and timestamp
    String safeTitle = title != null
        ? title.replaceAll("[^a-zA-Z0-9\\s-_]", "").replaceAll("\\s+", "_")
        : "document";
    if (safeTitle.length() > 50) {
      safeTitle = safeTitle.substring(0, 50);
    }

    String timestamp = String.valueOf(System.currentTimeMillis());
    String fileName = safeTitle + "_" + timestamp + ".html";

    // Create the full file path
    Path filePath = Paths.get(rootFolder, fileName);

    // Write the HTML content to the file
    Files.write(
        filePath,
        htmlContent.getBytes("UTF-8"),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);

    return fileName;
  }

  /** Load the HTML template from file. */
  private String loadTemplate() throws IOException {
    // Get the project assets folder using SEMOSS utility
    String projectId = this.insight.getProjectId();
    if (projectId == null) {
      projectId = this.insight.getContextProjectId();
    }

    if (projectId != null) {
      String assetsFolder = AssetUtility.getProjectAssetsFolder(projectId);
      Path templatePath = Paths.get(assetsFolder, "templates", "html_template.html");

      if (Files.exists(templatePath)) {
        return Files.readString(templatePath);
      }
    }

    throw new SemossPixelException(
        "HTML template not found. Project ID: "
            + projectId
            + ", Assets folder: "
            + (projectId != null ? AssetUtility.getProjectAssetsFolder(projectId) : "unknown"));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getParamMap() {
    GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
    if (mapGrs != null && !mapGrs.isEmpty()) {
      List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
      if (mapInputs != null && !mapInputs.isEmpty()) {
        return (Map<String, Object>) mapInputs.get(0).getValue();
      }
    }
    List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
    if (mapInputs != null && !mapInputs.isEmpty()) {
      return (Map<String, Object>) mapInputs.get(0).getValue();
    }
    return null;
  }

  private Map<String, Object> getJsonSchema() {
    Map<String, Object> titleProp = new HashMap<>();
    titleProp.put("type", "string");

    Map<String, Object> bodyProp = new HashMap<>();
    bodyProp.put("type", "string");

    Map<String, Object> properties = new HashMap<>();
    properties.put("title", titleProp);
    properties.put("body", bodyProp);

    List<String> required = Arrays.asList("title", "body");

    Map<String, Object> schema = new HashMap<>();
    schema.put("type", "object");
    schema.put("properties", properties);
    schema.put("required", required);
    schema.put("additionalProperties", false);

    return schema;
  }

  private Map<String, Object> parseResponse(String jsonString) {
    Type type = new TypeToken<Map<String, Object>>() {
    }.getType();
    Map<String, Object> map = null;

    try {
      map = GSON.fromJson(jsonString, type);
    } catch (JsonSyntaxException e) {
      throw new SemossPixelException("Failed to parse JSON response: " + jsonString, e);
    }
    return map;
  }

  /** Escape HTML special characters to prevent encoding issues. */
  private String escapeHtml(String input) {
    if (input == null) {
      return "";
    }
    return input
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;")
        .replace("'", "&#x27;")
        .replace("/", "&#x2F;");
  }

  /**
   * Sanitize HTML to prevent URL encoding/decoding issues when passed as
   * parameters.
   */
  private String sanitizeForUrlPassing(String html) {
    if (html == null) {
      return "";
    }

    return html
        // First, convert literal \n strings to actual newlines
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        // Then normalize whitespace - remove excessive newlines and spaces
        .replaceAll("\\r\\n", "\n") // Normalize line endings
        .replaceAll("\\r", "\n") // Normalize line endings
        .replaceAll("\\n\\s*\\n\\s*\\n+", "\n\n") // Remove 3+ consecutive newlines
        .replaceAll("[ \\t]+", " ") // Collapse multiple spaces/tabs to single space
        .replaceAll("\\n\\s+", "\n") // Remove leading spaces on new lines
        .replaceAll("\\s+\\n", "\n") // Remove trailing spaces before newlines
        // Remove problematic characters that might be interpreted as URL encoding
        .replaceAll("%(?![0-9A-Fa-f]{2})", "&#37;") // Escape standalone % characters
        .replaceAll("\\+(?![0-9])", "&#43;") // Escape + characters (except in numbers)
        .trim();
  }

  /**
   * Final cleanup of HTML to remove any remaining literal escape sequences and
   * normalize
   * formatting.
   */
  private String finalHtmlCleanup(String html) {
    if (html == null) {
      return "";
    }

    return html
        // Handle various forms of literal escape sequences
        .replace("\\\\n", "\n") // Double-escaped newlines
        .replace("\\n", "\n") // Single literal \n
        .replace("\\\\r", "\r") // Double-escaped carriage returns
        .replace("\\r", "\r") // Single literal \r
        .replace("\\\\t", "\t") // Literal tabs
        .replace("\\t", " ") // Convert tabs to spaces
        // Clean up excessive whitespace while preserving HTML structure
        .replaceAll("\\n{3,}", "\n\n") // Max 2 consecutive newlines
        .replaceAll("[ ]{2,}", " ") // Max 1 space between words
        // Clean up whitespace around HTML tags
        .replaceAll(">\\s+<", "><") // Remove space between tags
        .replaceAll(">\\n+<", "><") // Remove newlines between tags
        .trim();
  }

  @Override
  public String getReactorDescription() {
    return "Generates HTML content for documents using LLM based on user questions. Creates"
        + " professional documents with structured content including title, body, and"
        + " styling. Uses AI to generate comprehensive answers and formats them as clean"
        + " HTML suitable for document conversion. Creates an HTML file in the insight root"
        + " folder and returns the filename for further processing.";
  }

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(QUESTION_KEY)) {
      return "The question or topic for which to generate HTML content";
    } else if (key.equals(ENGINE_KEY)) {
      return "The LLM engine ID to use for content generation";
    } else if (key.equals(LOGO_URL_KEY)) {
      return "Optional URL for company logo to include in the HTML header";
    } else if (key.equals(ReactorKeysEnum.ROOM_ID.getKey())) {
      return "Optional room ID for conversation context";
    } else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
      return "Optional parameter map for LLM configuration";
    }
    return super.getDescriptionForKey(key);
  }
}
