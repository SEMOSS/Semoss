package prerna.rdf.query.util;

import java.util.Hashtable;

public class SPARQLCustomModifier implements ISPARQLReturnModifier{
	
	String modString;

	public void setModString(String modString) {
		this.modString = modString;
	}

	@Override
	public String getModifierAsString() {
		return modString;
	}
	
	@Override
	public void setLowerLevelModifier(
			Hashtable<String, ISPARQLReturnModifier> lowerMods) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Hashtable<String, ISPARQLReturnModifier> getLowerLevelModifier() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void setModID(String id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getModID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setModType(SPARQLModifierConstant modConstant) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getModType() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
