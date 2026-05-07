package prerna.reactor.platform;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Pixel endpoint for executing a platform default tool by name.
 *
 * <p>Used by the frontend when the LLM returns a tool call for a
 * {@code platform__*} tool (as opposed to an MCP engine tool).
 *
 * <p>Example Pixel:
 * <pre>
 *   RunDefaultTool(function=["platform__Command"], paramValues=[{"command": "ls -la"}]);
 * </pre>
 */
public class RunDefaultToolReactor extends AbstractReactor {

    public RunDefaultToolReactor() {
        this.keysToGet = new String[] {
            ReactorKeysEnum.FUNCTION.getKey(),
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
        };
        this.keyRequired = new int[] { 1, 0 };
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();

        String toolName = this.keyValue.get(ReactorKeysEnum.FUNCTION.getKey());
        if (toolName == null || (toolName = toolName.trim()).isEmpty()) {
            throw new IllegalArgumentException("Tool function name must be provided");
        }

        Map<String, Object> paramMap = getParamMap();
        String result = PlatformDefaultTools.execute(toolName, paramMap, this.insight);
        return new NounMetadata(result, PixelDataType.CONST_STRING, PixelOperationType.LITERAL);
    }

    private Map<String, Object> getParamMap() {
        GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
        if (mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if (mapInputs != null && !mapInputs.isEmpty()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> m = (Map<String, Object>) mapInputs.get(0).getValue();
                return m;
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if (mapInputs != null && !mapInputs.isEmpty()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) mapInputs.get(0).getValue();
            return m;
        }
        return null;
    }

    @Override
    public String getReactorDescription() {
        return "Execute a platform default tool by its prefixed name (e.g. platform__Command)";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.FUNCTION.getKey())) {
            return "The platform tool name, including the platform__ prefix (e.g. platform__Command)";
        } else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
            return "A key-value map of parameter inputs for the tool";
        }
        return super.getDescriptionForKey(key);
    }
}
