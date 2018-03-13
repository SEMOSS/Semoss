package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

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
		for(int i = 0; i < mapList.size(); i++) {
			Map<String, Object> mapOptions = (Map<String, Object>) mapList.get(i);
			String name = (String) mapOptions.get("name");
			nameCol[i] = name;
			String description = (String) mapOptions.get("description");
			descCol[i] = description;
			String rule = (String) mapOptions.get("rule");
			ruleCol[i] = rule;
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
		
		rsb.append(issueFrame + "<- data.frame("+issueFrameNameCol+","+issueFrameDescriptionCol+","+issueFrameRuleCol +");");
		System.out.println(rsb.toString());
		
		
		rsb.append("library(editrules);");

		// edit rules r script
		String editRulesScriptFilePath = getBaseFolder() + "\\R\\EditRules\\editRules2.R";
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
