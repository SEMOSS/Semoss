package prerna.sablecc2.om.nounmeta;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
	
	public void addAdditionalOpTypes(PixelOperationType... opType) {
		for(PixelOperationType op : opType) {
			this.opType.add(op);
		}
	}
	
	public void addAdditionalOpTypes(List<PixelOperationType> opType) {
		this.opType.addAll(opType);
	}
	
	public void addAdditionalReturn(NounMetadata noun) {
		this.additionalReturns.add(noun);
	}
	
	public void addAllAdditionalReturn(Collection<NounMetadata> nouns) {
		this.additionalReturns.addAll(nouns);
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
		return n;
		
//		Class<? extends Object> valueClass = value.getClass();
//		Object cloneValue = gson.fromJson(gson.toJson(value), valueClass);
//		return new NounMetadata(cloneValue, this.noun, this.opType);
	}

	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Static noun constructors
	 */
	
	/**
	 * Utility to get back a noun metadata with a warning message
	 * @param message
 	 * @param additionalOps			The default op type will be warning but can add additional ones
	 * @return
	 */
	public static NounMetadata getSuccessNounMessage(String message, PixelOperationType... additionalOps) {
		NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);
		noun.addAdditionalOpTypes(additionalOps);
		return noun;
	}
	
	
	/**
	 * Utility to get back a noun metadata with a warning message
	 * @param message
 	 * @param additionalOps			The default op type will be warning but can add additional ones
	 * @return
	 */
	public static NounMetadata getWarningNounMessage(String message, PixelOperationType... additionalOps) {
		NounMetadata noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.WARNING);
		noun.addAdditionalOpTypes(additionalOps);
		return noun;
	}
	
	/**
	 * Utility to get back a noun metadata with an error message
	 * @param message
	 * @param additionalOps			The default op type will be error but can add additional ones
	 * @return
	 */
	public static NounMetadata getErrorNounMessage(String message, PixelOperationType... additionalOps) {
		NounMetadata noun = new NounMetadata(message, PixelDataType.ERROR, PixelOperationType.ERROR);
		noun.addAdditionalOpTypes(additionalOps);
		return noun;
	}
	
	/**
	 * Utility to get back a noun metadata with an error message
	 * @param details
	 * @param additionalOps			The default op type will be error but can add additional ones
	 * @return
	 */
	public static NounMetadata getErrorNounMessage(Map details, PixelOperationType... additionalOps) {
		NounMetadata noun = new NounMetadata(details, PixelDataType.ERROR, PixelOperationType.ERROR);
		noun.addAdditionalOpTypes(additionalOps);
		return noun;
	}
}
