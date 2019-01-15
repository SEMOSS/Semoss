package prerna.sablecc2.reactor.frame.r.rules;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class RunRulesReactor extends AbstractRFrameReactor {
	public RunRulesReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.RULES_MAP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "validate", "settings", "yaml" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = frame.getMetaData();
		String dfName = frame.getName();
		String[] frameColumnNames = frame.getColumnNames();
		HashMap<String, Pattern> colRegexMap = new HashMap<String, Pattern>();
		List<Object> mapList = this.curRow.getValuesOfType(PixelDataType.MAP);
		StringBuilder rsb = new StringBuilder();
		// this frame holds the rule meta data
		String issueFrame = "issueFrame" + Utility.getRandomString(8);
		// this frame holds the rule summary
		// valid count, invalid count
		String dqFrame = "dqFrame" + Utility.getRandomString(8);
		int size = mapList.size();
		Object[] nameCol = new Object[size];
		Object[] descCol = new Object[size];
		Object[] ruleCol = new Object[size];
		Object[] inputColsListCol = new Object[size]; 
		HashMap<String, Object> validateRulesTemplate = null;

		// now that file is read in, we need to look through it to identify the
		// appropriate edit rule based on the path
		// look through the edit rules keyset to find the rule
		HeadersException colNameChecker = HeadersException.getInstance();
		ArrayList<String> headers = new ArrayList<String>(Arrays.asList(frameColumnNames));
		for (int i = 0; i < mapList.size(); i++) {
			Map<String, Object> mapOptions = (Map<String, Object>) mapList.get(i);
			// this will be the column header for dataframe
			String name = (String) mapOptions.get("name");
			name = colNameChecker.recursivelyFixHeaders(name, headers);
			headers.add(name);
			nameCol[i] = name;
			String description = (String) mapOptions.get("description");
			String rule = (String) mapOptions.get("rule");
			String definedRuleType = (String) mapOptions.get("definedRuleType");
			ArrayList<String> inputColsList = new ArrayList<String>();
			
			if (definedRuleType != null && !definedRuleType.isEmpty()) {
				// look up rule in template
				if (validateRulesTemplate == null) {
					// read in the edit rules file
					String fileJsonPath = getBaseFolder() + "\\R\\EditRules\\validateRulesTemplate.json";
					String jsonString = "";
					try {
						jsonString = new String(Files.readAllBytes(Paths.get(fileJsonPath)));
						validateRulesTemplate = new ObjectMapper().readValue(jsonString, HashMap.class);
					} catch (IOException e2) {
						throw new IllegalArgumentException("Unable to read file from path: " + fileJsonPath);
					}
				}
				
				HashMap<String, Object> ruleTemplate = (HashMap<String, Object>) validateRulesTemplate.get(mapOptions.get("definedRuleType"));
				if(ruleTemplate == null) {
					throw new IllegalArgumentException("definedRuleType: "+mapOptions.get("definedRuleType")+" is not a valid rule");

				}
				rule = (String) ruleTemplate.get("rule");
				rule = RSyntaxHelper.escapeRegexR(rule);
				HashMap<String, Object> columnTemplate = (HashMap<String, Object>) ruleTemplate.get("columns");
				HashMap<String, Object> columnParms = (HashMap<String, Object>) mapOptions.get("columns");
				
				if (columnTemplate.size() == columnParms.size()){
					// apply column params to rule
					for (String key : columnTemplate.keySet()) {
						String column = (String) columnParms.get(key);
						if (column != null) {
							// check columnType
							String dataType = meta.getHeaderTypeAsString(dfName + "__" + column);
							Map<String, Object> colMetaMap = (Map<String, Object>) columnTemplate.get(key);
							String requiredType = (String) colMetaMap.get("columnType");
							// check if dataTypes match for input col and template
							if ((Utility.isNumericType(requiredType) && Utility.isNumericType(dataType))
									|| (Utility.isStringType(requiredType) && Utility.isStringType(dataType))) {
								// go forward with edit rules
								inputColsList.add(column);
								inputColsListCol[i] = inputColsList;
								// update the rule to use the appropriate column name
								String targetString = "<" + key + ">";
								rule = rule.replace(targetString, column);
							} else {
								throw new IllegalArgumentException(column + " must be a " + requiredType);
							}
						}
					}
				} else {
					throw new IllegalArgumentException("Number of column parameters in the input does not match"
							+ "the number of columns defined in the rule template for rule: " + rule);
				}

				// get description
				description = (String) ruleTemplate.get("description");
			} else if (rule != null && !rule.isEmpty()) {
				// decode rule sent from fe
				rule = Utility.decodeURIComponent((String) rule); 
				rule = RSyntaxHelper.escapeRegexR(rule);
				// if rule is not null, then need to identify column(s) that the rule is targeting
				if (colRegexMap.isEmpty()) {
					for (int j = 0; j < frameColumnNames.length; j++) {
						String columnName = frameColumnNames[j];
						colRegexMap.put(columnName, Pattern.compile(columnName));
					}
				}
				for (HashMap.Entry<String, Pattern> entry : colRegexMap.entrySet()) {
					String columnName = entry.getKey();
					Pattern rx = entry.getValue();
					if (rx.matcher(rule).find()) {
						inputColsList.add(columnName);
					}
					inputColsListCol[i] = inputColsList;
				}
			} else {
				throw new IllegalArgumentException("Must specify ONE of the following: adhoc rule "
						+ "or the predefined rule name");
			}
			ruleCol[i] = rule;
			descCol[i] = description;
		}

		// create rule frame with the following columns: name, description, rule
		String issueFrameNameCol = "name" + Utility.getRandomString(8);
		rsb.append(issueFrameNameCol + "<-" + RSyntaxHelper.createStringRColVec(nameCol));
		rsb.append(";");
		String issueFrameDescriptionCol = "description" + Utility.getRandomString(8);
		rsb.append(issueFrameDescriptionCol + "<-" + RSyntaxHelper.createStringRColVec(descCol));
		rsb.append(";");
		String issueFrameRuleCol = "rule" + Utility.getRandomString(8);
		rsb.append(issueFrameRuleCol + "<-" + RSyntaxHelper.createStringRColVec(ruleCol));
		rsb.append(";");
		String issueFrameInputColsListCol = "inputColsList" + Utility.getRandomString(8);
		rsb.append(issueFrameInputColsListCol + "<-" + RSyntaxHelper.createStringRColVec(inputColsListCol));
		rsb.append(";");
		rsb.append(issueFrame + "<- data.frame(" + issueFrameNameCol + "," + issueFrameDescriptionCol + ","
				+ issueFrameRuleCol + "," + issueFrameInputColsListCol + ");");

		// validate rules r script
		String validateRulesScriptFilePath = getBaseFolder() + "\\R\\EditRules\\validate.R";
		validateRulesScriptFilePath = validateRulesScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + validateRulesScriptFilePath + "\");");

		// call validate rules function
		/// get the confront object and expanded issueFrame, with rule alias
		String list = "list" + Utility.getRandomString(8);
		rsb.append(list + " <- createCF(" + dfName + "," + issueFrame + ");");
		String cf = "cf" + Utility.getRandomString(8);
		rsb.append(cf + " <- " + list + "$cf;");
		rsb.append(issueFrame + " <- " + list + "$issueFrame;");
		rsb.append(dqFrame + "<- getDqFrame(" + cf + ", " + issueFrame + ");");

		// get errorFrame
		String errorFrame = "errorFrame" + Utility.getRandomString(8);
		rsb.append(errorFrame + "<- getErrorFrame(" + cf + ", " + issueFrame + ");");
		rsb.append(errorFrame + "[] <- lapply(" + errorFrame + ", function(x) (as.character(x)));");

		// add new colums frome error frame to frame
		rsb.append(dfName + " <- getDF(" + dfName + ", " + errorFrame + ");");
		System.out.println(rsb.toString());
		this.rJavaTranslator.runR(rsb.toString());

		// add new columns to meta
		// update the metadata to include this new column
		String[] ruleColumns = this.rJavaTranslator.getColumns(errorFrame);
		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + list + ");");
		cleanUpScript.append("rm(" + cf + ");");
		cleanUpScript.append("rm(" + issueFrameNameCol + ");");
		cleanUpScript.append("rm(" + issueFrameDescriptionCol + ");");
		cleanUpScript.append("rm(" + issueFrameRuleCol + ");");
		cleanUpScript.append("rm(" + issueFrameInputColsListCol + ");");
		// rm function names
		cleanUpScript.append("rm(createCF, getDqFrame, getErrorFrame, getDF, run.seq, escapeRegexR);");
		cleanUpScript.append("rm(" + errorFrame + ");");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		if (ruleColumns != null) {
			for (String newColName : ruleColumns) {
				meta.addProperty(dfName, dfName + "__" + newColName);
				meta.setAliasToProperty(dfName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dfName + "__" + newColName, "STRING");
			}
		} else {
			// no results
			throw new IllegalArgumentException("Invalid Rules");
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
