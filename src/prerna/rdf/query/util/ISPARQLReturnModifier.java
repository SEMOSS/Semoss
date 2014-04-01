package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Hashtable;

public interface ISPARQLReturnModifier {
	
	//this interface API does not support two modifiers on one level
	public final static String MOD = "MODIFIER";
	
	public void setLowerLevelModifier(Hashtable<String, ISPARQLReturnModifier> lowerMods);
	
	public Hashtable<String, ISPARQLReturnModifier> getLowerLevelModifier();
	
	public String getModifierAsString();
	
	public void setModID(String id);
	
	public String getModID();
	
	public void setModType(SPARQLModifierConstant modConstant);
	
	public String getModType();
	
}
