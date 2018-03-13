package prerna.sablecc2.reactor.frame.r;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunEditRulesReactor extends AbstractRFrameReactor {
	public RunEditRulesReactor() {
		this.keysToGet = new String[] { "rulesMap" };
	}

	@Override
	public NounMetadata execute() {
		init();
		RDataTable frame = (RDataTable) getFrame();
		String dfName = frame.getTableName();
		List<Object> mapList = this.curRow.getValuesOfType(PixelDataType.MAP);
		StringBuilder rsb = new StringBuilder();
		String issueFrame = "issueFrame";
		int size = mapList.size();
		Object[] nameCol = new Object[size];
		Object[] descCol = new Object[size];
		Object[] ruleCol = new Object[size];

		HashMap<String, Object> editRulesTemplate = null;

		// now that file is read in, we need to look through it to identify the
		// appropriate edit rule based on the path
		// look through the edit rules keyset to find the rule
		for (int i = 0; i < mapList.size(); i++) {
			Map<String, Object> mapOptions = (Map<String, Object>) mapList.get(i);
			String name = (String) mapOptions.get("name");
			nameCol[i] = name;
			String description = (String) mapOptions.get("description");
			String rule = (String) mapOptions.get("rule");
			if (rule == null) {

				// look up name in template
				if (editRulesTemplate == null) {
					// read in the edit rules file
					String fileJsonPath = getBaseFolder() + "\\R\\EditRules\\editRulesTemplate.json";
					String jsonString = "";
					try {
						jsonString = new String(Files.readAllBytes(Paths.get(fileJsonPath)));
						editRulesTemplate = new ObjectMapper().readValue(jsonString, HashMap.class);
					} catch (IOException e2) {
						throw new IllegalArgumentException("Unable to read file from path: " + fileJsonPath);
					}
				}
				HashMap<String, Object> ruleTemplate = (HashMap<String, Object>) editRulesTemplate.get(name);
				rule = (String) ruleTemplate.get("rule");
				HashMap<String, Object> columnTemplate = (HashMap<String, Object>) ruleTemplate.get("columns");
				HashMap<String, Object> columnParms = (HashMap<String, Object>) mapOptions.get("columns");

				// apply column params to rule
				for (String key : columnTemplate.keySet()) {
					String column = (String) columnParms.get(key);
					if (column != null) {
						// go forward with edit rules
						// update the rule to use the appropriate column name
						String targetString = "<" + key + ">";
						rule = rule.replace(targetString, column);
						System.out.println(rule + "");
					}
				}
				// get description
				description = (String) ruleTemplate.get("description");

			}
			ruleCol[i] = rule;
			descCol[i] = description;

		}

		// create rule frame with the following columns: name, description, rule
		String issueFrameNameCol = "name";
		rsb.append(issueFrameNameCol + "<-" + RSyntaxHelper.createStringRColVec(nameCol));
		rsb.append(";");
		String issueFrameDescriptionCol = "description";
		rsb.append(issueFrameDescriptionCol + "<-" + RSyntaxHelper.createStringRColVec(descCol));
		rsb.append(";");
		String issueFrameRuleCol = "rule";
		rsb.append(issueFrameRuleCol + "<-" + RSyntaxHelper.createStringRColVec(ruleCol));
		rsb.append(";");

		rsb.append(issueFrame + "<- data.frame(" + issueFrameNameCol + "," + issueFrameDescriptionCol + ","
				+ issueFrameRuleCol + ");");
		rsb.append("library(editrules);");

		// edit rules r script
		String editRulesScriptFilePath = getBaseFolder() + "\\R\\EditRules\\editRules.R";
		editRulesScriptFilePath = editRulesScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + editRulesScriptFilePath + "\");");

		// call edit rules function
		String editFrame = "editFrame" + Utility.getRandomString(8);
		rsb.append(editFrame + " <- getDqFrame(" + dfName + ", " + issueFrame + ");");
		System.out.println(rsb.toString());
		this.rJavaTranslator.runR(rsb.toString());
		
		return null;
	}

}
