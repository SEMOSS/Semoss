package prerna.sablecc2.reactor.qs.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;
import prerna.sablecc2.reactor.qs.selectors.IfReactor;

public class QueryFilterComponentSimple extends FilterReactor {

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
		// need to consider list of values
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

			// If I do this logic.. both of them become selectors and then we are all set
			// need to do special processing here
			// this is a special case where we need to turn the rset into the selector of the query struct
			// if I adjust this everything else gets adjusted
			if((this.parentReactor instanceof IfReactor || this.parentReactor instanceof IfQueryFilterComponentAnd || this.parentReactor instanceof IfQueryFilterComponentOr) && noun.getNounType() == PixelDataType.QUERY_STRUCT)
				noun = new NounMetadata(((SelectQueryStruct)noun.getValue()).getSelectors().get(0), PixelDataType.COLUMN);	

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
		
		if(lSet.isEmpty() && rSet.isEmpty()) {
			return null;
		}
		
		// DATE 2021-04-01 IS THIS STILL NEEDED?
		// TODO: throw warning that the filter for the query is invalid!
		// reason for warning is because for param insights where FE will
		// pass an empty set when the user selects all for a specific filter
		
		SimpleQueryFilter filter = new SimpleQueryFilter(getNounForFilter(lSet), comparator, getNounForFilter(rSet));
		return filter;
	}
	
	/**
	 * Get the appropriate nouns for each side of the filter expression
	 * @param nouns
	 * @return
	 */
	private NounMetadata getNounForFilter(List<NounMetadata> nouns) {
		NounMetadata noun = null;
		if(nouns.size() == 0) {
			noun = new NounMetadata(new ArrayList<>(), PixelDataType.CONST_STRING);
		} else if(nouns.size() > 1) {
			List<Object> values = new Vector<Object>();
			for(int i = 0; i < nouns.size(); i++) {
				NounMetadata subNoun = nouns.get(i);
				if(subNoun.getNounType() == PixelDataType.TASK || subNoun.getNounType() == PixelDataType.FORMATTED_DATA_SET) {
					values.addAll(TaskUtility.flushJobData(subNoun.getValue()));
				} else {
					if(subNoun.getValue() instanceof List) {
						values.addAll( (List) subNoun.getValue());
					} else {
						values.add(subNoun.getValue());
					}
				}
			}
			noun = new NounMetadata(values, TaskUtility.predictTypeFromObject(values));
		} else {
			noun = nouns.get(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.TASK || nounType == PixelDataType.FORMATTED_DATA_SET) {
				List<Object> values = TaskUtility.flushJobData(noun.getValue());
				noun = new NounMetadata(values, TaskUtility.predictTypeFromObject(values));
			}
		}
		
		return noun;
	}
	
	@Override
	public void mergeUp() {
		// merge this reactor into the parent reactor
		if(parentReactor != null) {
			// filters are added to curRow
			NounMetadata data = new NounMetadata(generateFilterObject(), PixelDataType.FILTER);
			parentReactor.getCurRow().add(data);
		}
	}
	
}
