package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import prerna.ds.r.RDataTable;
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
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < mapList.size(); i++) {
			Map<String, Object> mapOptions = (Map<String, Object>) mapList.get(i);
			String name = (String) mapOptions.get("name");
			String description = (String) mapOptions.get("description");
			String rule = (String) mapOptions.get("rule");
			sb.append(rule + "\n");
		}

		try {
			// write rules to edit file
			String editRulesFilePath = getBaseFolder() + "\\R\\EditRules\\" + Utility.getRandomString(8)+".txt";
			editRulesFilePath = editRulesFilePath.replace("\\", "/");
			PrintWriter pw = new PrintWriter(new File(editRulesFilePath));
			pw.write(sb.toString());
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

		
		return null;
	}

}
