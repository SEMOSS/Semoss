package prerna.sablecc2.om.nounmeta;

import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.gson.GsonUtility;

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
		
		Gson gson = GsonUtility.getDefaultGson();
		String str = gson.toJson(this);
		NounMetadata n = gson.fromJson(str, NounMetadata.class);
		
		Class<? extends Object> valueClass = value.getClass();
		Object cloneValue = cloneValue = gson.fromJson(gson.toJson(value), valueClass);
		
		return new NounMetadata(cloneValue, this.noun, this.opType);
	}
}
