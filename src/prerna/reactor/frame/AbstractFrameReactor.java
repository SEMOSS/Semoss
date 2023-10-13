package prerna.reactor.frame;

import java.util.List;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.poi.main.HeadersException;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractFrameReactor extends AbstractReactor {

	/**
	 * 
	 * @return
	 */
	protected ITableDataFrame getFrame() {
		GenRowStruct grs = this.store.getNoun(PixelDataType.FRAME.getKey());
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
		// put this into the noun store
		// so that we can pull it for other pipeline
		ITableDataFrame defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		if (defaultFrame != null) {
			this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
			return defaultFrame;
		}

		throw new NullPointerException("No frame found");
	}
	
	/**
	 * 
	 * @return
	 */
	protected ITableDataFrame getFrameDefaultLast() {
		GenRowStruct grs = this.store.getNoun(PixelDataType.FRAME.getKey());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			List<Object> frameInputs = grs.getValuesOfType(PixelDataType.FRAME);
			return (ITableDataFrame) frameInputs.get(0);
		}
		
		List<NounMetadata> curNouns = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(curNouns != null && !curNouns.isEmpty()) {
			return (ITableDataFrame) curNouns.get(0).getValue();
		}
		
		ITableDataFrame defaultFrame = null;
		// get the last frame
		SelectQueryStruct qs = this.insight.getLastQS(this.insight.getLastPanelId());
		if(qs != null) {
			defaultFrame = qs.getFrame();
		}
		if(defaultFrame != null) {
			return defaultFrame;
		}
		
		// else, grab the default frame from the insight
		// put this into the noun store
		// so that we can pull it for other pipeline
		defaultFrame = (ITableDataFrame) this.insight.getDataMaker();
		if (defaultFrame != null) {
			this.store.makeNoun(ReactorKeysEnum.FRAME.getKey()).add(new NounMetadata(defaultFrame, PixelDataType.FRAME));
			return defaultFrame;
		}

		throw new NullPointerException("No frame found");
	}
	
	/**
	 * 
	 * @return
	 */
	protected Set<ITableDataFrame> getAllFrames() {
		return this.insight.getVarStore().getAllCreatedFrames();
	}
	
	/**
	 * Replace all the references in the noun store to point to a specific frame
	 * @param frameNoun
	 */
	protected void setFrameInNounStore(NounMetadata frameNoun) {
		GenRowStruct grs = this.store.getNoun(PixelDataType.FRAME.getKey());
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
	protected String getCleanNewColName(String colName) {
		return getCleanNewColName(getFrame(), colName);
	}
	
	/**
	 * Quick method for {@link #getColNames(ITableDataFrame, String)}
	 * using passed in frame
	 * @param frameName
	 * @param colName
	 * @return
	 */
	protected String[] getColNames() {
		return getColNames(getFrame());
	}
	
	protected String getCleanNewColName(ITableDataFrame frame, String colName) {
		// make the new column name valid
		HeadersException colNameChecker = HeadersException.getInstance();
		String[] currentColumnNames = getColNames(frame);
		String validNewHeader = colNameChecker.recursivelyFixHeaders(colName, currentColumnNames);
		return validNewHeader;
	}
	
	protected String[] getColNames(ITableDataFrame frame) {
		List<String> colNames = frame.getMetaData().getOrderedAliasOrUniqueNames();
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
