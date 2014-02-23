package prerna.poi.main;

public class RelationSheet {

	String Relation;
	String Header1;
	String Header2;
	
	public RelationSheet(String Relation1, String Header11, String Header21){
		Relation = Relation1;
		Header1 = Header11;
		Header2 = Header21;
	}
	public String getRelation() {
		return Relation;
	}
	public void setRelation(String relation) {
		Relation = relation;
	}
	public String getHeader1() {
		return Header1;
	}
	public void setHeader1(String header1) {
		Header1 = header1;
	}
	public String getHeader2() {
		return Header2;
	}
	public void setHeader2(String header2) {
		Header2 = header2;
	}
	
}
