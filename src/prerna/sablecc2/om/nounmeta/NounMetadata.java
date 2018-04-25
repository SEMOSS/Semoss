package prerna.sablecc2.om.nounmeta;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.date.SemossDate;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.gson.NumberAdapter;
import prerna.util.gson.SemossDateAdapter;

public class NounMetadata {
	
	Object value;
	PixelDataType noun;
	List<PixelOperationType> opType = new Vector<PixelOperationType>();

	String explanation = "";
	List<NounMetadata> additionalReturns = new Vector<NounMetadata>();
	
	/**
	 * Default constructor for preset nouns
	 */
	NounMetadata() {
		
	}
	
	public NounMetadata(Object value, PixelDataType noun) {
		this(value, noun, PixelOperationType.OPERATION);
	}
	
	public NounMetadata(Object value, PixelDataType noun, PixelOperationType opType) {
		this.noun = noun;
		this.value = value;
		this.opType.add(opType);
	}
	
	public NounMetadata(Object value, PixelDataType noun, PixelOperationType... opType) {
		this.noun = noun;
		this.value = value;
		for(PixelOperationType op : opType) {
			this.opType.add(op);
		}
	}
	
	public NounMetadata(Object value, PixelDataType noun, List<PixelOperationType> opType) {
		this.noun = noun;
		this.value = value;
		this.opType.addAll(opType);
	}
	
	public Object getValue() {
		return this.value;
	}
	
	public PixelDataType getNounType() {
		return this.noun;
	}
	
	public List<PixelOperationType> getOpType() {
		return this.opType;
	}
	
	public void addAdditionalReturn(NounMetadata noun) {
		this.additionalReturns.add(noun);
	}
	
	public List<NounMetadata> getAdditionalReturn() {
		return this.additionalReturns;
	}
	
	public void setExplanation(String explanation) {
		this.explanation = explanation;
	}
	
	public String getExplanation() {
		return this.explanation;
	}
	
	/**
	 * To help w/ debugging
	 */
	public String toString() {
		return "NOUN META DATA ::: " + this.value + "";
	}
	
	public NounMetadata copy() {
		// I cannot copy a null noun
		if(this.noun == PixelDataType.NULL_VALUE) {
			return this;
		}

		GsonBuilder gsonBuilder = new GsonBuilder()
				.disableHtmlEscaping()
				.excludeFieldsWithModifiers(Modifier.TRANSIENT)
				.registerTypeAdapter(Double.class, new NumberAdapter())
				.registerTypeAdapter(SemossDate.class, new SemossDateAdapter());

		gsonBuilder = IQuerySelector.appendQueryAdapters(gsonBuilder);		
		Gson gson = gsonBuilder.create();
		
		Class<? extends Object> valueClass = value.getClass();
		String jsonStr = gson.toJson(value);
		Object cloneValue = null;
		
		// we need some crazy stuff for query selectors as they are recursive
		if(this.noun == PixelDataType.COLUMN) {
			IQuerySelector.SELECTOR_TYPE type = ((IQuerySelector) this.value).getSelectorType();
			cloneValue = (IQuerySelector) IQuerySelector.getGson().fromJson(jsonStr, IQuerySelector.getQuerySelectorClassFromType(type));
		} else {
			cloneValue = gson.fromJson(jsonStr, valueClass);
		}
		
		return new NounMetadata(cloneValue, this.noun, this.opType);
	}
}
