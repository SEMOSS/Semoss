package prerna.query.querystruct.update;

import java.util.ArrayList;
import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;

public class UpdateQueryStruct extends AbstractQueryStruct {
	
	private List<Object> values = new ArrayList<>();
	
	/**
	 * Default constructor
	 */
	public UpdateQueryStruct() {
		
	}
	
	//////////////////////////////////////////// SELECTORS /////////////////////////////////////////////////
	
	@Override
	public void addSelector(IQuerySelector selector) {
		if(selector.getSelectorType() !=  IQuerySelector.SELECTOR_TYPE.COLUMN) {
			throw new IllegalArgumentException("Can only add column selector for update queries");
		}
		this.selectors.add(selector);
	}
	
	//////////////////////////////////////////// VALUES ////////////////////////////////////////////////////
	
	public List<Object> getValues() {
		return this.values;
	}
	
	public void setValues(List<Object> values) {
		this.values = values;
	}
	
	/**
	 * 
	 * @param incomingQS
	 * This method is responsible for merging "incomingQS's" data with THIS querystruct
	 */
	public void merge(AbstractQueryStruct incomingQS) {
		super.merge(incomingQS);
		if(incomingQS instanceof UpdateQueryStruct) {
			UpdateQueryStruct updateQS = (UpdateQueryStruct) incomingQS;
			mergeValues(updateQS.values);
		}
	}

	private void mergeValues(List<Object> values) {
		for(Object val : values) {
			if(!this.values.contains(val)) {
				this.values.add(val);
			}
		}
	}
	
	/**
	 * Gets a new QS with the base information moved over
	 * This is basically the qs type + enginename + csv/excel properties
	 * Note csv/excel qs overrides this method
	 * @return
	 */
	public UpdateQueryStruct getNewBaseQueryStruct() {
		UpdateQueryStruct newQs = new UpdateQueryStruct();
		newQs.setQsType(this.qsType);
		if(this.qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
			newQs.setEngineId(this.engineId);
			newQs.setEngine(this.engine);
		} else if(this.qsType == SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			newQs.setFrame(this.frame);
		}
		newQs.setValues(this.values);
		return newQs;
	}
	
}
