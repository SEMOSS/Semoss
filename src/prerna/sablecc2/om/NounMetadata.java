package prerna.sablecc2.om;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class NounMetadata 
{
	private String explanation = "";
	private final PixelDataType noun;
	private final List<PixelOperationType> opType = new Vector<PixelOperationType>();
	private final Object value;

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
	
	public PixelDataType getNounType() {
		return this.noun;
	}
	
	public List<PixelOperationType> getOpType() {
		return this.opType;
	}
	
	public void setExplanation(String explanation) {
		this.explanation = explanation;
	}
	
	public String explanation() {
		return this.explanation;
	}
	
	public Object getValue() {
		return this.value;
	}
	
	/**
	 * To help w/ debugging
	 */
	public String toString() {
		return "NOUN META DATA ::: " + this.value.toString();
	}
	
	public NounMetadata copy() {
		Gson gson = new GsonBuilder().disableHtmlEscaping().excludeFieldsWithModifiers(Modifier.TRANSIENT).create();
		Class<? extends Object> valueClass = value.getClass();
		Object cloneValue = gson.fromJson(gson.toJson(value), valueClass);
		return new NounMetadata(cloneValue, this.noun, this.opType);
	}
}
