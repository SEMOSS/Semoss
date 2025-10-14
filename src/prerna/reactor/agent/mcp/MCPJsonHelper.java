
package prerna.reactor.agent.mcp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class MCPJsonHelper {

    /**
     * Updates a list of tools with a new tool. If a tool with the same name exists, it's replaced. Otherwise, the new tool is added.
     *
     * @param tool The tool to add or replace, as a JSONObject.
     * @param tools The list of existing tools as a JSONArray.
     */
    public void updateTools(JSONObject tool, JSONArray tools) {
        String toolName = tool.getString("name");
        int toolIndex = -1;
        for (int i = 0; i < tools.length(); i++) {
            JSONObject existingTool = tools.getJSONObject(i);
            String existingToolName = existingTool.getString("name");
            if (existingToolName != null && existingToolName.equals(toolName)) {
                toolIndex = i;
                break;
            }
        }

        if (toolIndex != -1) {
            tools.put(toolIndex, tool);
        } else {
            tools.put(tool);
        }
    }

    /**
     * Reads a JSON file containing a list of tools.
     *
     * @param filePath The path to the JSON file.
     * @return A JSONObject representing the JSON content.
     * @throws IOException If there is an error reading the file.
     */
    public JSONObject readMcpJson(String filePath) throws IOException {
        String content = FileUtils.readFileToString(new File(filePath), StandardCharsets.UTF_8);
        return new JSONObject(content);
    }

    /**
     * Writes a JSONObject to a JSON file.
     *
     * @param filePath The path to the JSON file.
     * @param mcpJson The JSONObject to write.
     * @throws IOException If there is an error writing the file.
     */
    public void writeMcpJson(String filePath, JSONObject mcpJson) throws IOException {
        String content = mcpJson.toString(4);
        FileUtils.writeStringToFile(new File(filePath), content, StandardCharsets.UTF_8);
    }
}
