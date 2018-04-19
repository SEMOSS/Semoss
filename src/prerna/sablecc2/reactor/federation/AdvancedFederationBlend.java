package prerna.sablecc2.reactor.federation;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdvancedFederationBlend extends AbstractRFrameReactor {
	public static final String JOIN_TYPE = "joinType";
	public static final String FRAME_COLUMN = "frameCol";
	public static final String DB_TABLE_COL = "dbTableCol";
	public static final String MAX_DISTANCE = "maxDist";
	public static final String ADDITIONAL_COLS = "additionalCols";

	public AdvancedFederationBlend() {
		this.keysToGet = new String[] { JOIN_TYPE, FRAME_COLUMN, DB_TABLE_COL, MAX_DISTANCE, ADDITIONAL_COLS };
	}

	@Override
	public NounMetadata execute() {
		
		init();
		organizeKeys();
		String joinType = this.keyValue.get(this.keysToGet[0]);
		String frameCol = this.keyValue.get(this.keysToGet[1]);
		String dbTableCol = this.keyValue.get(this.keysToGet[2]);
		String maxDist = this.keyValue.get(this.keysToGet[3]);
		String columns = this.keyValue.get(this.keysToGet[4]);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		List<String> dbTabColList = Arrays.asList(dbTableCol.split("__"));
		String newDb = dbTabColList.get(dbTabColList.size() - 3);
		String newTable = dbTabColList.get(dbTabColList.size() - 2);
		String newCol = dbTabColList.get(dbTabColList.size() - 1);
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		// get max distance
		int finalMax = 0;
		if (maxDist != null && !(maxDist.isEmpty())) {
			List<String> allMaxs = getMaxDist();
			for (int i = 0; i < allMaxs.size(); i++) {
				List<String> maxs = Arrays.asList(maxDist.split("___"));
				int distInstance = Integer.parseInt(maxs.get(maxs.size() - 1));
				if (distInstance > finalMax) {
					finalMax = distInstance;
				}
			}
		}

		String rCol1 = "ADVANCED_FEDERATION_FRAME_COL1";
		String rCol2 = "ADVANCED_FEDERATION_FRAME_COL2";
		String trg = "FED_TARGET";
		IEngine newColEngine = Utility.getEngine(newDb);

		// execute the link script to generate final table of links allowed
		String linkScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_fedeartion_blend.r\") ; "
				+ "LinkFrame <- best_match_maxdist(" + rCol1 + "," + rCol2 + ", " + finalMax + "); ";
		linkScript = linkScript.replace("\\", "/");

		this.rJavaTranslator.runR(linkScript);

		// get R Frame
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getTableName();

		// get all columns to federate in
		ArrayList<String> columnArray = new ArrayList<String>();
		columnArray.add(newCol);
		List<String> inputCols = new ArrayList<String>();
		if (columns != null && !(columns.isEmpty())) {
			inputCols.addAll(getColumns());
			columnArray.addAll(inputCols);
		}

		Map typesMap = new HashMap<String, SemossDataType>();
		QueryStruct2 qs = new QueryStruct2();
		qs.setEngine(newColEngine);
		for (int i = 0; i < columnArray.size(); i++) {
			String col = columnArray.get(i);
			QueryColumnSelector selector = new QueryColumnSelector();
			selector.setTable(newTable);
			selector.setColumn(col);
			selector.setAlias(col);
			qs.addSelector(selector);
			// get semoss type, update meta data and keep track
			String conceptDataType = MasterDatabaseUtility.getBasicDataType(newDb, col, newTable);
			SemossDataType semossType = SemossDataType.convertStringToDataType(conceptDataType);
			typesMap.put(col, semossType);
			// update meta data in frame
			metaData.addProperty(frameName, frameName + "__" + col);
			metaData.setAliasToProperty(frameName + "__" + col, col);
			metaData.setDataTypeToProperty(frameName + "__" + col, semossType.toString());
		}

		// write iterator data to csv, then read csv into R table as trg
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(newColEngine, qs);
		String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".csv";
		File newFile = Utility.writeResultToFile(newFileLoc, it, typesMap);
		String loadFileRScript = RSyntaxHelper.getFReadSyntax(trg, newFile.getAbsolutePath());
		this.rJavaTranslator.runR(loadFileRScript);
		newFile.delete();

		// execute blend
		String blendedFrameScript = frameName + " <- blend(" + frameName + ", \"" + frameCol + "\"," + trg + ",\"" + newCol + "\", LinkFrame , \"" + joinType + "\")";
		this.rJavaTranslator.runR(blendedFrameScript);

		// remove columns from frame that are temporary
		String removeExtras = frameName + " <- " + frameName + "[,-c(\"id\",\"col1\",\"col2\",\"dist.y\",\"dist.x\")]";
		this.rJavaTranslator.runR(removeExtras);

		// reset the frame headers
		this.getFrame().syncHeaders();

		// delete advanced Fed frame in R, done with it
		this.rJavaTranslator
				.runR("rm(" + trg + "," + rCol1 + "," + rCol2 + "," + "LinkFrame, ADVANCED_FEDERATION_FRAME)");

		// TODO: what happens if the new headers coming in are the same as
		// existing headers?
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata((inputCols)));
		return retNoun;
	}

	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[4]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}

		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for (Object obj : values) {
			strValues.add(obj.toString());
		}
		return strValues;
	}

	private List<String> getMaxDist() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for (Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}

		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for (Object obj : values) {
			strValues.add(obj.toString());
		}
		return strValues;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JOIN_TYPE)) {
			return "The join type for federating (inner, outer, left, etc.)";
		} else if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to use for joining.";
		} else if (key.equals(DB_TABLE_COL)) {
			return "The target database to join with.";
		} else if (key.equals(MAX_DISTANCE)) {
			return "The maximum distance between join elements (ex. abc = abcd -- distance is 1).";
		} else if (key.equals(ADDITIONAL_COLS)) {
			return "Additional columns to pull join with the existing frame.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
