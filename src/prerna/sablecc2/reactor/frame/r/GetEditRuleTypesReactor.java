package prerna.sablecc2.reactor.frame.r;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetEditRuleTypesReactor extends AbstractRFrameReactor {

	@Override
	public NounMetadata execute() {
		String fileJsonPath = getBaseFolder() + "\\R\\EditRules\\validateRulesTemplate.json";
        String jsonString = "";
        HashMap<String, Object> editRules = new HashMap<String,Object>();
        try {
            jsonString = new String(Files.readAllBytes(Paths.get(fileJsonPath)));
            editRules = new ObjectMapper().readValue(jsonString, HashMap.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to read file from path: " + fileJsonPath);
        }
		return new NounMetadata(editRules,PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

}
