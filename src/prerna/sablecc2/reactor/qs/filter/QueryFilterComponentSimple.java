package prerna.sablecc2.reactor.qs.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.task.ITask;

public class QueryFilterComponentSimple extends QueryFilterReactor {

	@Override
	public NounMetadata execute() {
		SimpleQueryFilter filter = generateFilterObject();
		return new NounMetadata(filter, PixelDataType.FILTER);
	}
	
	/**
	 * Generate the filter object that will be used by the query struct
	 * @return
	 */
	private SimpleQueryFilter generateFilterObject() {
		// need to consider list fo values
		// can have column == [set of values]
		// can also have [set of values] == column
		// need to account for both situations
		
		List<NounMetadata> lSet = new ArrayList<NounMetadata>();
		List<NounMetadata> rSet = new ArrayList<NounMetadata>();

		String comparator = null;
		boolean foundComparator = false;
		for(NounMetadata noun : this.curRow.vector) {
			// if we are at the comparator
			// store it and we are done for this loop
			if(noun.getNounType() == PixelDataType.COMPARATOR) {
				comparator = noun.getValue().toString().trim();
				foundComparator = true;
				continue;
			}
			
			// if we have the comparator
			// everything from this point on gets added to the rSet
			if(foundComparator) {
				rSet.add(noun);
			} else {
				// if we have not found the comparator
				// we are still at the left hand side of this expression
				lSet.add(noun);
			}
		}
		
		SimpleQueryFilter filter = null;
		if(!lSet.isEmpty() && !rSet.isEmpty()) {
			filter = new SimpleQueryFilter(getNounForFilter(lSet), comparator, getNounForFilter(rSet));
		} else {
			// TODO: throw warning that the filter for the query is invalid!
			// reason for warning is because for param insights where FE will
			// pass an empty set when the user selects all for a specific filter
		}
		return filter;
	}
	
	/**
	 * Get the appropriate nouns for each side of the filter expression
	 * @param nouns
	 * @return
	 */
	private NounMetadata getNounForFilter(List<NounMetadata> nouns) {
		NounMetadata noun = null;
		if(nouns.size() > 1) {
			List<Object> values = new Vector<Object>();
			for(int i = 0; i < nouns.size(); i++) {
				NounMetadata subNoun = nouns.get(i);
				if(subNoun.getNounType() == PixelDataType.TASK) {
					values.addAll(flushJobData((ITask) subNoun.getValue()));
				} else {
					if(subNoun.getValue() instanceof List) {
						values.addAll( (List) subNoun.getValue());
					} else {
						values.add(subNoun.getValue());
					}
				}
			}
			noun = new NounMetadata(values, predictTypeFromObject(values));
		} else {
			noun = nouns.get(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.TASK) {
				List<Object> values = flushJobData((ITask) noun.getValue());
				noun = new NounMetadata(values, predictTypeFromObject(values));
			}
		}
		
		return noun;
	}
	
	/**
	 * Flush the task data into an array
	 * This assumes you have table data!!!
	 * @param taskData
	 * @return
	 */
	private List<Object> flushJobData(ITask taskData) {
		List<Object> flushedOutCol = new ArrayList<Object>();
		// iterate through the task to get the table data
		List<Object[]> data = taskData.flushOutIteratorAsGrid();
		int size = data.size();
		// assumes we are only flushing out the first column
		for(int i = 0; i < size; i++) {
			flushedOutCol.add(data.get(i)[0]);
		}
		
		return flushedOutCol;
	}
	
	/**
	 * We got to predict the type of the values when we have a bunch to merge
	 * @param obj
	 * @return
	 */
	private PixelDataType predictTypeFromObject(List<Object> obj) {
		int size = obj.size();
		if(size == 0) {
			return PixelDataType.CONST_STRING;
		}
		
		Object firstVal = null;
		int counter = 0;
		while(firstVal == null && counter < size) {
			firstVal = obj.get(counter);
			counter++;
		}
		
		if(firstVal instanceof Double) {
			return PixelDataType.CONST_DECIMAL;
		} else if(firstVal instanceof Integer) {
			return PixelDataType.CONST_INT;
		} else if(firstVal instanceof String) {
			return PixelDataType.CONST_STRING;
		} else if(firstVal instanceof Boolean) {
			return PixelDataType.BOOLEAN;
		}
		
		return PixelDataType.CONST_STRING;
	}
	

}
