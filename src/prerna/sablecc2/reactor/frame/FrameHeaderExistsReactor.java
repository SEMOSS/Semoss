package prerna.sablecc2.reactor.frame;

import java.util.HashMap;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class FrameHeaderExistsReactor extends AbstractReactor {

	public FrameHeaderExistsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get the frame
		ITableDataFrame dm = getFrame();
		if (dm == null) {
			NounMetadata noun = new NounMetadata(new HashMap<String, Object>(), PixelDataType.CUSTOM_DATA_STRUCTURE);
			return noun;
		}
		// get the types of the headers requested
		String header = getHeader();
		
		List<String> aliasNames = dm.getMetaData().getFrameColumnNames();
		List<String> qsNames = dm.getMetaData().getFrameSelectors();

		// compare against both
		boolean hasHeader = false;
		if(aliasNames.contains(header) || qsNames.contains(header)) {
			hasHeader = true;
		}
		
		NounMetadata noun = new NounMetadata(hasHeader, PixelDataType.BOOLEAN);
		return noun;
	}

	/**
	 * Getting the frame that is required
	 * @return
	 */
	private ITableDataFrame getFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<Object> frameValues = this.curRow.getValuesOfType(PixelDataType.FRAME);
		if(frameValues != null && !frameValues.isEmpty()) {
			return (ITableDataFrame) frameValues.get(0);
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
	}
	
	/**
	 * Getting the frame that is required
	 * @return
	 */
	private String getHeader() {
		GenRowStruct colGrs = this.store.getNoun(this.keysToGet[1]);
		if(colGrs != null && !colGrs.isEmpty()) {
			return (String) colGrs.get(0);
		}
		
		List<Object> strValues = this.curRow.getValuesOfType(PixelDataType.CONST_STRING);
		if(strValues != null && !strValues.isEmpty()) {
			return (String) strValues.get(0);
		}
		
		strValues = this.curRow.getValuesOfType(PixelDataType.COLUMN);
		if(strValues != null && !strValues.isEmpty()) {
			return (String) strValues.get(0);
		}

		throw new IllegalArgumentException("Must input a column to match on");	
	}
	

}
