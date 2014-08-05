package prerna.rdf.query.util;

public class SPARQLModifierConstant {

	String constant;
	public SPARQLModifierConstant (Object constant)
	{
		this.constant = (String)constant;
	}
	
	public String getConstant()
	{
		return constant;
	}
	
	public void setConstant(String constant)
	{
		this.constant = constant;
	}

	@Override
	public boolean equals(Object obj){
		if(obj instanceof SPARQLModifierConstant){
			if(this.constant.equals(((SPARQLModifierConstant) obj).constant)){
				return true;
			}
		}
		return false;
	}
}
