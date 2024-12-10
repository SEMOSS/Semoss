package prerna.testing.utility;

import java.time.LocalDateTime;

public class TestExcelInputObject {

	private boolean isNull = false;
	private Integer i;
	private String s;
	private Double d;
	private LocalDateTime ldt;
	private Boolean b;
	private TestExcelType type;

	public TestExcelInputObject() {
		
	}

	public boolean isNull() {
		return isNull;
	}

	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}

	public Integer getI() {
		return i;
	}

	public void setI(Integer i) {
		this.i = i;
	}

	public String getS() {
		return s;
	}

	public void setS(String s) {
		this.s = s;
	}

	public Double getD() {
		return d;
	}

	public void setD(Double d) {
		this.d = d;
	}

	public LocalDateTime getLdt() {
		return ldt;
	}

	public void setLdt(LocalDateTime ldt) {
		this.ldt = ldt;
	}

	public Boolean getB() {
		return b;
	}

	public void setB(Boolean b) {
		this.b = b;
	}

	public TestExcelType getType() {
		return type;
	}

	public void setType(TestExcelType type) {
		this.type = type;
	}
	
	
	
}
