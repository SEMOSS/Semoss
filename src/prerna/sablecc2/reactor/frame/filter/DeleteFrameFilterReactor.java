package prerna.sablecc2.reactor.frame.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteFrameFilterReactor extends AbstractFilterReactor {

	public DeleteFrameFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.INDEX.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		GenRowFilters filters = null;

		// get the existing filters
		if (frame != null) {
			filters = frame.getFrameFilters();
		} else {
			throw new IllegalArgumentException("No frame is defined in the insight to remove the filters from");
		}

		// first try to delete based on index
		List<Integer> indexList = getOrderedIndexes();
		// first we need to delete the highest index in order to not change the index of what we are deleting
		for (int i = indexList.size(); i > 0; i--) {
			// remove the filter at the index specified by the index list
			filters.removeFilter(indexList.get(i - 1).intValue());
		}

		return new NounMetadata(filters, PixelDataType.FILTER, PixelOperationType.FRAME_FILTER);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	/**
	 * return an ordered list of the filter indexes that we want to delete
	 * 
	 * @return
	 */
	private List<Integer> getOrderedIndexes() {
		List<Integer> indexList = new ArrayList<Integer>();
		// this grs will contain all indexes, each as a separate noun
		GenRowStruct formatGRS = this.store.getNoun(keysToGet[0]);
		if (formatGRS != null && formatGRS.size() > 0) {
			for (int i = 0; i < formatGRS.size(); i++) {
				indexList.add(((Number) formatGRS.getNoun(i).getValue()).intValue());
			}
		} else {
			List<Object> numericInputs = this.curRow.getAllNumericColumns();
			for (int i = 0; i < numericInputs.size(); i++) {
				indexList.add(((Number) numericInputs.get(i)).intValue());
			}
		}
		// sort so that we can later remove based on the highest first
		if (indexList.isEmpty()) {
			throw new IllegalArgumentException("No indices are defined");
		}
		Collections.sort(indexList);
		return indexList;
	}

}
