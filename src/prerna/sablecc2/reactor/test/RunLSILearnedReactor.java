package prerna.sablecc2.reactor.test;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Left;

import java.util.Iterator;

import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;


import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.Utility;

public class RunLSILearnedReactor extends AbstractRFrameReactor{

	public RunLSILearnedReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NUMERIC_VALUE.getKey()};
	}
	@Override
	public NounMetadata execute() {
		// get pixel inputs
		organizeKeys();
		// get R connection
		init();
		String[] packages = new String[] { "LSAfun", "text2vec"};
		this.rJavaTranslator.checkPackages(packages);
		// output frame name
		RDataTable frame = (RDataTable) getFrame();
		String returnTable = frame.getName();
		
		String frameJoinCol = this.keyValue.get(this.keysToGet[0]);
		int numRows = Integer.parseInt(this.keyValue.get(this.keysToGet[1]));

		// path to your custom r script
		String rScriptPath2 = getBaseFolder() + "\\R\\UserScripts\\RunLSILearned2.r"; 
		String rScriptPath1 = getBaseFolder() + "\\R\\UserScripts\\lsi_lookup_learned.r"; 
		rScriptPath1 = rScriptPath1.replace("\\", "/");
		rScriptPath2 = rScriptPath2.replace("\\", "/");
		
		// embed r script in java
		StringBuilder rsb = new StringBuilder();
		// load r packages
		
		rsb.append("numMatch<-"+numRows+";");
		rsb.append("source(\"" + rScriptPath1 + "\");");
		String readDescriptions = "Description<-data.frame(gsub(\"_\",\" \"," + returnTable + "[," + frameJoinCol + "]));";
		rsb.append(readDescriptions);
		String alterFrameSpace = returnTable + "$" + frameJoinCol + "<-gsub(\" \",\"_\"," + returnTable + "[," + frameJoinCol + "]);";
		rsb.append(alterFrameSpace);
		rsb.append("LSAspace <- readRDS(\"lsalearned.rds\");");
		rsb.append("source(\"" + rScriptPath2 + "\");");
		
		
		String leftTableName = returnTable;
		//TODO Change dfFinal to random string generated. Edit the R script. 
		String rightTableName = "dfFinal";
		
		//TODO Change dfFinal to random string generated. Edit the R script. 

		rsb.append("dfFinal$Description<-gsub(\" \",\"_\"," + "dfFinal$Description);");
		rsb.append(returnTable+"$LSA_Score<-NULL;"+returnTable+"$LSA_Category<-NULL;"+returnTable+"$Match<-NULL;");
		// only a single join type can be passed at a time
		String joinType = "left.outer.join";
		List<Map<String, String>> joinCols = new ArrayList<Map<String, String>>();

			Map<String, String> joinColMapping = new HashMap<String, String>();
			//TODO Change Description to something else
			joinColMapping.put(frameJoinCol,"Description");
			joinCols.add(joinColMapping);
		
		
		//execute r command
		String mergeString = RSyntaxHelper.getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols);
		rsb.append(mergeString);
		rsb.append(";");
		
		//run script
		this.rJavaTranslator.runR(rsb.toString());
	
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);

	}

}


