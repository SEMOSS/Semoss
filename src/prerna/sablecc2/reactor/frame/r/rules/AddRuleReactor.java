package prerna.sablecc2.reactor.frame.r.rules;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class AddRuleReactor extends AbstractRFrameReactor {

	/**
	 * This reactor runs the adds rules to the validateRulesTemplate.json
	 * The reactor takes as an input a map: 
	 * AddEditRule([{"testRule":{"rule":"<encode>grepl(\"^-\\\\d+$\", <x>) 
	 * 	== TRUE</encode>","columns":{"x":{"description":"the description for x","columnType":"STRING"}},
	 *  "description":"my description"}}]);
	 */
	
	public AddRuleReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.RULES_MAP.getKey()};
	}	
	
	@Override
	public NounMetadata execute() {
		// initialize the rJava translator
		init();
		// grab the user input and get into map format
		List<Object> ruleList = this.curRow.getValuesOfType(PixelDataType.MAP);
		Map<String, Object> mapOptions = (Map<String, Object>) ruleList.get(0);

		// get the name of the rule
		// it is the value in the key set
		String name = "";
		Set<String> names = mapOptions.keySet();
		for (String item : names) {
			name = item;
		}

		// get input to the map
		HashMap<String, Object> mapOptionsEntry = (HashMap<String, Object>) mapOptions.get(name);
		// get the rule
		Object rule = (String) mapOptionsEntry.get("rule");
		rule = Utility.decodeURIComponent((String) rule);
		mapOptionsEntry.replace("rule", rule);
		
		// create string builder for the r script
		StringBuilder rsb = new StringBuilder();

		// before we add to the json, we need to ensure that the rule is valid
		// in order to validate testRule, we will need to (1) determine if rule is a regex and if so add 1 
		// additional escape character, where applicable, to make the regex R-syntax acceptable 
		// and (2) replace the <variableNames> with just the 
		// variableNames (remove the brackets)
		String testRule = (String) rule;
		//(1) if applicable, adding escape character(s) to regex 		
		testRule = RSyntaxHelper.escapeRegexR(testRule);

		//(2) get column name from columnMap
		Map<String, Object> columnMap = (Map<String, Object>) mapOptionsEntry.get("columns");
		for (String column : columnMap.keySet()) {
			// the unbracketed rule will be the test rule
			testRule = testRule.replaceAll("<" + column + ">", column);
		}
		
		String testRuleVar = "testRule" + Utility.getRandomString(8);
		rsb.append(testRuleVar + "<-\"" + testRule + "\";");
		
		// source the r function that will verify the rules validity
		String validateRuleScriptFilePath = getBaseFolder() + "\\R\\EditRules\\validateRule.R";
		validateRuleScriptFilePath = validateRuleScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + validateRuleScriptFilePath + "\");");

		// call the function in the r script and get the return value
		String rsbString = rsb.toString();
		this.rJavaTranslator.runR(rsbString);
        // script for running the function
		// when we validate, use the testRule
		String rScript = "as.character(validateRule(" + testRuleVar + "));";

		// isValid will either be true or false and indicate if the rule is valid based on editrules in r
		String isValid = this.rJavaTranslator.getString(rScript);
		
		// rm function names
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + testRuleVar + ");");
		cleanUpScript.append("rm(validateRule, escapeRegexR);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		// only go forward with updating the json if the rule is valid
		if (isValid.equalsIgnoreCase("true")) {
			// if the rule is valid, then we add to the json
			// first read the existing json so that we can append
			String fileJsonPath = getBaseFolder() + "\\R\\EditRules\\validateRulesTemplate.json";
			String jsonString = "";

			HashMap<String, Object> validateRulesTemplate = null;
			try {
				jsonString = new String(Files.readAllBytes(Paths.get(fileJsonPath)));
				validateRulesTemplate = new ObjectMapper().readValue(jsonString, HashMap.class);
			} catch (IOException e2) {
				throw new IllegalArgumentException("Unable to read file from path: " + fileJsonPath);
			}
			
			// before adding, we should see if the name already exists in error rules template
			// if it does exist, we should throw an error and tell the user
			if (validateRulesTemplate.keySet().contains(name)) {
				throw new IllegalArgumentException("The name of the rule already exists. Please use a different name for the rule.");
			}

			// we need to add our new map the the editRulesTemplate
			validateRulesTemplate.put(name, mapOptionsEntry);
			
			// create the json string based on the map
			// use the GsonBuilder for proper formatting (and to avoid encoding) of the json
			GsonBuilder builder = new GsonBuilder();
			builder.disableHtmlEscaping();
			builder.setPrettyPrinting();
			Gson gson = builder.create();
			String json = gson.toJson(validateRulesTemplate);

            // override the file with the new updated json
			PrintWriter pw;
			try {
				pw = new PrintWriter(new File(fileJsonPath));
				pw.write(json.toString());
				pw.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			throw new IllegalArgumentException("Invalid rule addition");
		}
		return null;
	}
	
}
