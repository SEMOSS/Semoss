package prerna.sablecc2.reactor.test;


import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;

public class LSASpaceColumnLearnedReactor extends AbstractRFrameReactor {

	/**
	 * This reactor creates an LSI space based on a column selected from the frame
	 * The inputs to the reactor are: 
	 * 1) the column to create the LSI Space
	 */
	
	public LSASpaceColumnLearnedReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// initialize the rJavaTranslator
		init();
		String[] packages = new String[] { "LSAfun", "text2vec"};
		this.rJavaTranslator.checkPackages(packages);
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get frame name
		String table = frame.getName();
		
		// create a string builder to keep track of the scripts to execute
		StringBuilder sb = new StringBuilder();
		String rScriptPath = getBaseFolder() + "\\R\\UserScripts\\lsi_lookup_learned.r"; 
		rScriptPath = rScriptPath.replace("\\", "/");
		sb.append("library(lsa);");

		sb.append("source(\"" + rScriptPath + "\");");

		// get inputs
		String lsaCol = getLSAColumn();
		String categoryCol = getCategoryColumn();
		// separate the column name from the frame name
		if (lsaCol.contains("__")) {
			lsaCol = lsaCol.split("__")[1];
		} 
		 
		String createLSA = "lookup_tbl<-data.frame((gsub(\"_\",\" \"," + table + "[," + lsaCol + "])),(gsub(\"_\",\" \"," + table + "[," + categoryCol + "])));";
		
		sb.append(createLSA);
		sb.append("lsi_mgr(lookup_tbl,0.8,\"lsalearned\");");
	sb.append("LSAspace <- readRDS(\"lsalearned.rds\");");
		//execute the r scripts
		if (sb.length() > 0) {
			this.rJavaTranslator.runR(sb.toString());
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	
	private String getLSAColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to generate LSA
			NounMetadata noun1 = inputsGRS.getNoun(0);
			String fullLSAColumn = noun1.getValue() + "";
			if (fullLSAColumn.length() == 0) {
				throw new IllegalArgumentException("Need to define column to create an LSA Space");
			}
			return fullLSAColumn;
		}
		throw new IllegalArgumentException("Need to define column to create an LSA Space");
	}
	
	private String getCategoryColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first noun will be the column to generate LSA
			NounMetadata noun1 = inputsGRS.getNoun(1);
			String fullCategoryColumn = noun1.getValue() + "";
			if (fullCategoryColumn.length() == 0) {
				throw new IllegalArgumentException("Need to define category column to create an LSA Space");
			}
			return fullCategoryColumn;
		}
		throw new IllegalArgumentException("Need to define category column to create an LSA Space");
	}

}
