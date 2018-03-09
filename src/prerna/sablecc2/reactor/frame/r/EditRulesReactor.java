package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * This reactor applies edit rules to a data frame
 * 
 * Input: 1) the column 2) the type of column (e.g., EditRules(column =
 * "PersonAge", dataType = "age");
 */

public class EditRulesReactor extends AbstractRFrameReactor {
	public EditRulesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), "type" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		RDataTable frame = (RDataTable) getFrame();
		String dfName = frame.getTableName();
		// check r packages
		String[] packages = new String[] { "lpSolveAPI", "igraph", "editRules" };
		// retrieve the column input
		String column = this.keyValue.get(this.keysToGet[0]);
		// retrieve the type of column
		String type = this.keyValue.get(this.keysToGet[1]);

		// read in the edit rules file
		String fileJsonPath = getBaseFolder() + "\\R\\EditRules\\editRulesTemplate.json";
		String jsonString = "";

		HashMap<String, Object> editRulesTemplate = null;
		try {
			jsonString = new String(Files.readAllBytes(Paths.get(fileJsonPath)));
			editRulesTemplate = new ObjectMapper().readValue(jsonString, HashMap.class);
		} catch (IOException e2) {
			throw new IllegalArgumentException("Unable to read file from path: " + fileJsonPath);
		}

		// now that file is read in, we need to look through it to identify the
		// appropriate edit rule based on the path
		// look through the edit rules keyset to find the rule
		Object typeRule = editRulesTemplate.get(type);
		HashMap<String, String> inputMap = (HashMap<String, String>) typeRule;

		String columnType = inputMap.get("columnType");
		String rule = inputMap.get("rule");

		// before updating the rule, check to make sure that the column is the
		// appropriate type for the rule (e.g., an age column should be numeric)
		// determine the column type of the column in the data frame

		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		String columnTypeInFrame = metadata.getHeaderTypeAsString(dfName + "__" + column);

		boolean typeMatch = false;
		if (columnType.equalsIgnoreCase("NUMBER")) {
			if (Utility.isNumericType(columnTypeInFrame)) {
				typeMatch = true;
			}
		} else {
			if (columnTypeInFrame.equalsIgnoreCase(columnType)) {
				typeMatch = true;
			}
		}

		if (typeMatch) {
			// go forward with edit rules
			// update the rule to use the appropriate column name
			String targetString = "<" + type + ">";
			String updatedRule = rule.replace(targetString, column);
			System.out.println(updatedRule);
			
			try {
				// write rule to edit file
				String editRulesFilePath = getBaseFolder() + "\\R\\EditRules\\" + Utility.getRandomString(8)+".txt";
				editRulesFilePath = editRulesFilePath.replace("\\", "/");
				PrintWriter pw = new PrintWriter(new File(editRulesFilePath));
				pw.write(updatedRule);
				pw.close();
				
				
				StringBuilder rsb = new StringBuilder();
				rsb.append("library(editrules);");
				// read edit file
				String editFile = "editFile" + Utility.getRandomString(8);
				rsb.append(editFile + " <- editfile(\"" + editRulesFilePath + "\");");

				// edit rules r script
				String editRulesScriptFilePath = getBaseFolder() + "\\R\\EditRules\\editRules.R";
				editRulesScriptFilePath = editRulesScriptFilePath.replace("\\", "/");
				rsb.append("source(\"" + editRulesScriptFilePath + "\");");

				// call edit rules function
				String editFrame = "editFrame" + Utility.getRandomString(8);
				rsb.append(editFrame + " <- editRules(" + dfName + ", " + editFile + ");");
				System.out.println(rsb.toString());
				this.rJavaTranslator.runR(rsb.toString());
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


		} else {
			throw new IllegalArgumentException("Column not of type: " + columnType);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.CODE_EXECUTION);
	}

}
