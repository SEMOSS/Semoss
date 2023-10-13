package prerna.reactor.frame.py;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class DivisionReactor extends AbstractPyFrameReactor {
	private static final String NUMERATOR = "numerator";
	private static final String DENOMINATOR = "denominator";
	private static final String ROUND = "round";

	public DivisionReactor() {
		this.keysToGet = new String[] { NUMERATOR, DENOMINATOR, ROUND, ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String frameName = frame.getName();
		
		// get the wrapper name
		// which is the framename with w in the end
		String wrapperFrameName = frame.getWrapperName();
		
		// get inputs
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
		String[] startingColumns = getColumns(frame);
		List<String> startingColumnsList = new Vector<String>(startingColumns.length);
		startingColumnsList.addAll(Arrays.asList(startingColumns));
		if (numerator == null || !startingColumnsList.contains(numerator) || denominator == null
				|| !startingColumnsList.contains(denominator)) {
			throw new IllegalArgumentException("Need to define existing numerator and denominator columns.");
		}

		// create script
		String script = wrapperFrameName + ".col_division('" + numerator + "', '" + denominator + "', '" + newColName + "')";
		
		// run script
		insight.getPyTranslator().runEmptyPy(script);
		this.addExecutedCode(script);
		
		/*insight.getPyTranslator().runEmptyPy(wrapperFrameName + ".cache['data']['" + newColName + "'] = " +
				 wrapperFrameName + ".cache['data'].apply(lambda x: x['" + numerator + "']/x['" + denominator + "'] "
				 		+ "if ("
				 		+ "(isinstance(x['" + numerator + "'], int) or isinstance(x['" + numerator + "'], float) )  and "
				 		+ "(isinstance(x['" + denominator + "'], int) or isinstance(x['" + denominator + "'], float) )  and "
				 		+ "x['" + denominator + "'] != 0"
				 		+ ") else 0 , axis = 1)");
		*/
		// check if new column exists
		
		String[] endingColumns = getColumns(frame);
		List<String> endingColumnsList = new Vector<String>(endingColumns.length);
		endingColumnsList.addAll(Arrays.asList(endingColumns));
		if (!endingColumnsList.contains(newColName)) {
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

