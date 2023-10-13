package prerna.reactor.frame.r;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DivisionReactor extends AbstractRFrameReactor {
	private static final String NUMERATOR = "numerator";
	private static final String DENOMINATOR = "denominator";
	private static final String ROUND = "round";
	
	public DivisionReactor() {
		this.keysToGet = new String[] { NUMERATOR, DENOMINATOR, ROUND, ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();

		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		String numerator = this.keyValue.get(NUMERATOR);
		String denominator = this.keyValue.get(DENOMINATOR);
		String round = this.keyValue.get(ROUND);
		if(round == null || round.isEmpty()) {
			round = "2";
		} else {
			// make sure positive #
			int num = 0;
			try {
				num = ((Number) Double.parseDouble(round)).intValue();
			} catch(Exception e) {
				throw new IllegalArgumentException("Must pass in a valid number for round");
			}
			if(num < 0) {
				throw new IllegalArgumentException("Round value must be > 0");
			}
		}
		String newColName = this.keyValue.get(ReactorKeysEnum.NEW_COLUMN.getKey());
		newColName = getCleanNewColName(newColName);

		// make sure column exists
		String[] startingColumns = getColumns(frameName);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		if (numerator == null || !startingColumnsList.contains(numerator) || denominator == null
				|| !startingColumnsList.contains(denominator)) {
			throw new IllegalArgumentException("Need to define existing numerator and denominator columns.");
		}

		// create R script
		StringBuilder script = new StringBuilder();
		script.append(frameName).append("$").append(newColName).append(" <- round(")
			.append(frameName).append("$").append(numerator).append("/")
			.append(frameName).append("$").append(denominator).append(", digits = ")
			.append(round).append(");");

		// run R script
		this.rJavaTranslator.runR(script.toString());
		this.addExecutedCode(script.toString());

		// check if new column exists
		String colExistsScript = "\"" + newColName + "\" %in% colnames(" + frameName + ")";
		boolean colExists = this.rJavaTranslator.getBoolean(colExistsScript);
		if (!colExists) {
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to perform column division");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// update meta data to add new column
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		metaData.addProperty(frameName, frameName + "__" + newColName);
		metaData.setAliasToProperty(frameName + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frameName + "__" + newColName, SemossDataType.DOUBLE.toString());
		metaData.setDerivedToProperty(frameName + "__" + newColName, true);
		frame.syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully performed column division"));
		return retNoun;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(NUMERATOR)) {
			return "The column to use as the numerator";
		} else if (key.equals(DENOMINATOR)) {
			return "The column to use as the denominator";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
