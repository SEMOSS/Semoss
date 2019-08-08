package prerna.sablecc2.reactor.frame;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class AbstractFrameReactor extends AbstractReactor {

	protected ITableDataFrame getFrame() {
		GenRowStruct grs = this.store.getNoun(PixelDataType.FRAME.toString());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			List<Object> frameInputs = grs.getValuesOfType(PixelDataType.FRAME);
			return (ITableDataFrame) frameInputs.get(0);
		}
		
		grs = this.store.getNoun(ReactorKeysEnum.FRAME.getKey());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			List<Object> frameInputs = grs.getValuesOfType(PixelDataType.FRAME);
			return (ITableDataFrame) frameInputs.get(0);
		}
		
		List<NounMetadata> curNouns = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(curNouns != null && !curNouns.isEmpty()) {
			return (ITableDataFrame) curNouns.get(0).getValue();
		}
		
		// else, grab the default frame from the insight
		if (this.insight.getDataMaker() != null) {
			return (ITableDataFrame) this.insight.getDataMaker();
		}

		throw new NullPointerException("No frame found");
	}
	
	/**
	 * Replace all the references in the noun store to point to a specific frame
	 * @param frameNoun
	 */
	protected void setFrameInNounStore(NounMetadata frameNoun) {
		GenRowStruct grs = this.store.getNoun(PixelDataType.FRAME.toString());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			grs.removeValuesOfType(PixelDataType.FRAME);
			grs.add(frameNoun);
		}
		
		grs = this.store.getNoun(ReactorKeysEnum.FRAME.getKey());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			grs.removeValuesOfType(PixelDataType.FRAME);
			grs.add(frameNoun);
		}
		
		List<NounMetadata> curNouns = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(curNouns != null && !curNouns.isEmpty()) {
			this.curRow.removeValuesOfType(PixelDataType.FRAME);
			this.curRow.add(frameNoun);
		}
	}
	
	/**
	 * Quick method for {@link #getCleanNewColName(ITableDataFrame, String, String)}
	 * using passed in frame
	 * @param frameName
	 * @param colName
	 * @return
	 */
	protected String getCleanNewColName(String frameName, String colName) {
		return getCleanNewColName(getFrame(), frameName, colName);
	}
	
	/**
	 * Quick method for {@link #getColNames(ITableDataFrame, String)}
	 * using passed in frame
	 * @param frameName
	 * @param colName
	 * @return
	 */
	protected String[] getColNames(String frameName) {
		return getColNames(getFrame(), frameName);
	}
	
	protected String getCleanNewColName(ITableDataFrame frame, String frameName, String colName) {
		// make the new column name valid
		HeadersException colNameChecker = HeadersException.getInstance();
		String[] currentColumnNames = getColNames(frame, frameName);
		String validNewHeader = colNameChecker.recursivelyFixHeaders(colName, currentColumnNames);
		return validNewHeader;
	}
	
	protected String[] getColNames(ITableDataFrame frame, String frameName) {
		List<String> colNames = frame.getMetaData().getFrameColumnNames();
		String[] colString = new String[colNames.size()];
		for (int i = 0; i < colNames.size(); i++) {
			String column = colNames.get(i);
			if (column.contains("__")) {
				column = colNames.get(i).split("__")[1];
			}
			colString[i] = colNames.get(i);
		}
		return colString;
	}
}
