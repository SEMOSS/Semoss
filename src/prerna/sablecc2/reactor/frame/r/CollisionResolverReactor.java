package prerna.sablecc2.reactor.frame.r;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Utility;

public class CollisionResolverReactor extends AbstractRFrameReactor {
	public static final String DISTANCE_KEY = "dist";
	public static final String COLUMN_KEY = "column";
	private static final String CLASS_NAME = CollisionResolverReactor.class.getName();

	@Override
	public NounMetadata execute() {
		init();
		RDataTable frame = (RDataTable) getFrame();
		Logger logger = this.getLogger(CLASS_NAME);
		frame.setLogger(logger);
		String df = frame.getTableName();
		String column = getColumn();

		StringBuilder rsb = new StringBuilder();
		// create temp R frame with column values
		String randomDF = Utility.getRandomString(8);
		rsb.append(randomDF + " <- data.frame(" + column + "=unique(" + df + "$" + column + "));");

		// add column instance count
		String countScript = randomDF + "$count <- NA; ";
		countScript += "for (i in 1:nrow(" + randomDF + ")) { value <- " + randomDF + "$" + column
				+ "[i]; count <- length(which(" + df + "$" + column + " == value)); " + randomDF
				+ "$count[i] <-count};";
		rsb.append(countScript);

		// load collision resolver script
		String collisionScriptPath = getBaseFolder() + "\\R\\UserScripts\\CollisionResolver.r";
		collisionScriptPath = collisionScriptPath.replace("\\", "/");
		rsb.append("source(\"" + collisionScriptPath + "\");");

		// fuzzy join parameters
		String method = "jw";
		double maxdist = getDistance();
		maxdist = 1 - maxdist;
		String maxDistStr = "" + maxdist;
		String join = "inner";

		// build the collision command write to json variable
		String outputJSON = Utility.getRandomString(8);
		rsb.append(outputJSON);
		rsb.append("<-  collision_resolver(");
		rsb.append(randomDF + ",");
		rsb.append("\"" + column + "\"" + ",");
		rsb.append("\"" + join + "\"" + ",");
		rsb.append(maxDistStr + ",");
		rsb.append("method =" + "\"" + method + "\"" + ",");
		rsb.append("q=" + "0" + ",");
		rsb.append("p=" + "0" + ");");

		// run scripts
		long startTime = System.currentTimeMillis();
		this.rJavaTranslator.runR(rsb.toString());
		long endTime = System.currentTimeMillis();
		logger.info("Time to execute R script" + (endTime - startTime));

		// get json string this string contains \n characters
		// so parse into json object here
		String json = this.rJavaTranslator.getString(outputJSON);
		List<Object> jsonMap = new ArrayList<Object>();
		if (json != null) {
			try {
				// parse json here
				jsonMap = new ObjectMapper().readValue(json, List.class);
			} catch (IOException e2) {
			}
		}
		return new NounMetadata(jsonMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

	private String getColumn() {
		String column = "";
		GenRowStruct grs = this.store.getNoun(COLUMN_KEY);
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			column = (String) noun.getValue();
		}
		return column;
	}

	/**
	 * Get the similarity threshold 0-1 (fuzzy-exact)
	 * 
	 * @return
	 */
	private double getDistance() {
		double distance = 0.8;
		GenRowStruct grs = this.store.getNoun(DISTANCE_KEY);
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			distance = (double) noun.getValue();
		}
		return distance;
	}

}
