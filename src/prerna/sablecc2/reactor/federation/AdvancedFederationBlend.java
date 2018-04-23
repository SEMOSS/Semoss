package prerna.sablecc2.reactor.federation;

import java.io.File;
import java.util.ArrayList;
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
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdvancedFederationBlend extends AbstractRFrameReactor {
	public static final String JOIN_TYPE = "joinType";
	public static final String FRAME_COLUMN = "frameCol";
	public static final String MATCHES = "matches";
	public static final String ADDITIONAL_COLS = "additionalCols";

	public AdvancedFederationBlend() {
		this.keysToGet = new String[] { JOIN_TYPE, FRAME_COLUMN, ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), MATCHES, ADDITIONAL_COLS };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get all inputs
		String joinType = this.keyValue.get(this.keysToGet[0]);
		String frameCol = this.keyValue.get(this.keysToGet[1]);
		String newDb = this.keyValue.get(this.keysToGet[2]);
		String newTable = this.keyValue.get(this.keysToGet[3]);
		String newCol = this.keyValue.get(this.keysToGet[4]);
		List<String> allMatches = getMatches();
		String columns = this.keyValue.get(this.keysToGet[6]);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		final String rCol1 = "ADVANCED_FEDERATION_FRAME_COL1";
		final String rCol2 = "ADVANCED_FEDERATION_FRAME_COL2";
		final String matchesFrame = "ADVANCED_FEDERATION_FRAME";
		final String trg = "FED_TARGET";

		// check if packages are installed
		String[] packages = {"jsonlite", "stringdist"};
		this.rJavaTranslator.checkPackages(packages);


		// execute the link script to generate base frame of 0 distance matches
		// and merge with LinkFrame built in BestMatches
		String linkScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\") ; "
				+ "BaseLinkFrame <- best_match_maxdist(" + rCol1 + "," + rCol2 + ", 0 ); LinkFrame <- rbind(LinkFrame, BaseLinkFrame);";
		linkScript = linkScript.replace("\\", "/");
		this.rJavaTranslator.runR(linkScript);
		
		// lookup all matches data and generate script to blend
		StringBuilder col1Builder = new StringBuilder();
		StringBuilder col2Builder = new StringBuilder();
		StringBuilder col3Builder = new StringBuilder();
		String rand = Utility.getRandomString(8);
		if (allMatches != null && !(allMatches.isEmpty())) {
			for (int i = 0; i < allMatches.size(); i++) {
				if (i != 0) {
					col1Builder.append(",");
					col2Builder.append(",");
					col3Builder.append(",");
				}
				String match = (String) allMatches.get(i);
				String col1 = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match + "\"), ]$col1)");
				String col2 = this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match + "\"), ]$col2)");
				int dist = Integer.parseInt(this.rJavaTranslator.getString("as.character(" + matchesFrame + "[" + matchesFrame + "$combined %in% c(\"" + match + "\"), ]$distance)"));
				col1Builder.append("\"" + col1 + "\"");
				col2Builder.append("\"" + col2 + "\"");
				col3Builder.append(dist);
			}
			
			// add all matches provided
			String script =  rand + " <- data.frame(\"col1\"=c(" + col1Builder + "), \"col2\"=c(" + col2Builder + "), \"dist\"=c(" + col3Builder + ")); LinkFrame <- rbind(LinkFrame," + rand + ");";
			this.rJavaTranslator.runR(script);
		}
		
		// get R Frame
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getTableName();

		// get all columns to federate on
		ArrayList<String> columnArray = new ArrayList<String>();
		columnArray.add(newCol);
		List<String> inputCols = new ArrayList<String>();
		if (columns != null && !(columns.isEmpty())) {
			inputCols.addAll(getColumns());
			columnArray.addAll(inputCols);
		}

		IEngine newColEngine = Utility.getEngine(newDb);
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
			OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
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
		this.rJavaTranslator.runR("rm(" + trg + "," + rCol1 + "," + rCol2 + "," + rand + ", BaseLinkFrame, LinkFrame, ADVANCED_FEDERATION_FRAME)");

		// TODO: what happens if the new headers coming in are the same as
		// existing headers?
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata((inputCols)));
		return retNoun;
	}

	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[6]);
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

		// else, we assume it is values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for (Object obj : values) {
			strValues.add(obj.toString());
		}
		return strValues;
	}

	private List<String> getMatches() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(MATCHES);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<String> values = columnGrs.getAllStrValues();
				return values;
			}
		}
		// else, we assume it is values in the curRow
		List<String> values = this.curRow.getAllStrValues();
		return values;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(JOIN_TYPE)) {
			return "The join type for federating (inner, outer, left, etc.)";
		} else if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to use for joining.";
		} else if (key.equals(MATCHES)) {
			return "The matches of columns that are selected by the user.";
		} else if (key.equals(ADDITIONAL_COLS)) {
			return "Additional columns to pull join with the existing frame.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
