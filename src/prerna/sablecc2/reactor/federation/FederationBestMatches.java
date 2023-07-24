package prerna.sablecc2.reactor.federation;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IDatabase;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/*
 * Please use {@link #FuzzyMatchesReactor.class}
 */

@Deprecated
public class FederationBestMatches extends AbstractRFrameReactor {
	private static final Logger logger = LogManager.getLogger(FederationBestMatches.class);
	
	public static final String FRAME_COLUMN = "frameCol";	
	public static final String OUTPUT_FRAME_NAME = "outputFrame";
	
	public FederationBestMatches() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey(), FRAME_COLUMN, OUTPUT_FRAME_NAME};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		this.rJavaTranslator.checkPackages(packages);

		// for the first iteration we have to build the inputs, second iteration
		// we already have them
		String newDatabase = this.keyValue.get(this.keysToGet[0]);
		String newTable = this.keyValue.get(this.keysToGet[1]);
		String newCol = this.keyValue.get(this.keysToGet[2]);
		String frameCol = this.keyValue.get(this.keysToGet[3]);
		
		// 4 column results df with matches, distance, and combined column
		final String matchesFrame = getMatchesName();
		// 1 column df of all data in frame join column
		final String rCol1 = matchesFrame + "col1";
		// 1 column df of all data in the incoming join column
		final String rCol2 = matchesFrame + "col2";
		
		// accept input info, generate matches table
		IDatabase newColDatabase = Utility.getEngine(newDatabase);
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		String rTable1 = rCol1 + " <- as.character(" + frameName + "$" + frameCol + ");";

		// create script to generate col2 from table to be joined
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setEngine(newColDatabase);

		// we will fill these once we figure out if it is a concept or property
		QueryColumnSelector selector = null;
		String conceptDataType = null;
		// this is a hack
		// since i dont know if it is a concept or a property
		// if i get a valid data type, new col is a property for new table
		// if i dont, then newtable is a concept with a prim key that i need to use
		// update - switching logic for different method
		if(newColDatabase.getPhysicalUriFromPixelSelector(newTable + "__" + newCol) == null) {
			// we couldn't find a parent for this property
			// this means it is a concept itself
			// and we should only use table
			selector = new QueryColumnSelector(newTable);
			conceptDataType = MasterDatabaseUtility.getBasicDataType(newDatabase, newTable, null);
		} else {
			selector = new QueryColumnSelector(newTable + "__" + newCol);
			conceptDataType = MasterDatabaseUtility.getBasicDataType(newDatabase, newCol, newTable);
		}
		// add the selector to the qs
		qs.addSelector(selector);

		// get the info to write this data to a tsv
		Map typesMap = new HashMap<String, SemossDataType>();
		SemossDataType semossType = SemossDataType.convertStringToDataType(conceptDataType);
		typesMap.put(newCol, semossType);
		String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";

		// exec query
		File newFile = null;
		IRawSelectWrapper it2 = null;
		try {
			it2 = WrapperManager.getInstance().getRawWrapper(newColDatabase, qs);
			// write to file
			 newFile = Utility.writeResultToFile(newFileLoc, it2, typesMap, "\t");
		} catch (Exception e) {
			logger.error("StackTrace: ", e);
		} finally {
			if(it2 != null) {
				it2.cleanUp();
			}
		}

		if (newFile != null) {
			String loadFileRScript = rCol2 + " <- fread(\"" + newFile.getAbsolutePath().replace("\\", "/") + "\", sep=\"\t\");";
			this.rJavaTranslator.runR(loadFileRScript);
			this.rJavaTranslator.runR(rCol2 + " <- as.character(" + rCol2 + "$" + newCol + ")");
			newFile.delete();
		}
		
		// execute the scripts
		this.rJavaTranslator.executeEmptyR(rTable1);

		// generate script based on what george wants - empty list of selected
		String bestMatchScript = "source(\"" + baseFolder + "\\R\\Recommendations\\advanced_federation_blend.r\") ; "
				+ matchesFrame + " <- best_match(" + rCol1 + "," + rCol2 + ");";
		bestMatchScript = bestMatchScript.replace("\\", "/");

		this.rJavaTranslator.runR(bestMatchScript);

		// add a unique combined col1 == col2, remove extra columns,
		String combineScript = matchesFrame + "$distance <- as.numeric(" + matchesFrame + "$dist);" + matchesFrame
				+ "<-" + matchesFrame + "[,c(\"col1\",\"col2\",\"distance\")]; " + matchesFrame + "<-" + matchesFrame
				+ "[order(unique(" + matchesFrame + ")$distance),] ;";

		this.rJavaTranslator.runR(combineScript + matchesFrame + " <- as.data.table(" + matchesFrame + ");");

		//convert col1/col2 from factor to list
		String convertType = matchesFrame+"$col1<-as.character("+matchesFrame+"$col1);"+matchesFrame+"$col2<-as.character("+matchesFrame+"$col2);";
		this.rJavaTranslator.runR(convertType);
		
		// remove all garbage 
		this.rJavaTranslator.runR("rm(" + rCol1 + "," + rCol2 + ")");
		
	
		RDataTable returnTable = createNewFrameFromVariable(matchesFrame);
		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);
		
		// get count of exact matches
		String exactMatchCount = this.rJavaTranslator.getString("as.character(nrow(" + matchesFrame + "[" + matchesFrame + "$distance == 0,]))");
		if (exactMatchCount != null){
			int val = Integer.parseInt(exactMatchCount);
			retNoun.addAdditionalReturn(new NounMetadata(val, PixelDataType.CONST_INT));
		} else{
			throw new IllegalArgumentException("No matches found.");
		}
		
		this.insight.getVarStore().put(matchesFrame, retNoun);
		return retNoun;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FRAME_COLUMN)) {
			return "The column from the existing frame to join on";
		} else if(key.equals(OUTPUT_FRAME_NAME)){
			return "Specify the output frame name";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	
	private String getMatchesName() {
		String matchesFrame = this.keyValue.get(this.keysToGet[4]);
		if(matchesFrame == null || matchesFrame.isEmpty()) {
			matchesFrame = Utility.getRandomString(8) + "adFed";
		}
		return matchesFrame;
	}
	
	public String getName()
	{
		return "FederationBestMatches";
	}
	
}
