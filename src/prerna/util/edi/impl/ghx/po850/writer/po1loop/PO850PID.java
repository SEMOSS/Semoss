package prerna.util.edi.impl.ghx.po850.writer.po1loop;

import prerna.util.edi.IX12Format;

public class PO850PID implements IX12Format {

	private String pid01 = "F";
	private String pid02 = "";
	private String pid03 = "";
	private String pid04 = "";
	private String pid05 = "";
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		
		String builder = "PID" 
				+ elementDelimiter + pid01
				+ elementDelimiter + pid02
				+ elementDelimiter + pid03
				+ elementDelimiter + pid04
				+ elementDelimiter + pid05
				+ segmentDelimiter;
		
		return builder;
	}
	
	// setters/getter

	public String getPid01() {
		return pid01;
	}

	public PO850PID setPid01(String pid01) {
		this.pid01 = pid01;
		return this;
	}
	
	public PO850PID setItemDescriptionType(String pid01) {
		this.pid01 = pid01;
		return this;
	}

	public String getPid02() {
		return pid02;
	}

	public PO850PID setPid02(String pid02) {
		this.pid02 = pid02;
		return this;
	}

	public String getPid03() {
		return pid03;
	}

	public PO850PID setPid03(String pid03) {
		this.pid03 = pid03;
		return this;
	}

	public String getPid04() {
		return pid04;
	}

	public PO850PID setPid04(String pid04) {
		this.pid04 = pid04;
		return this;
	}

	public String getPid05() {
		return pid05;
	}

	public PO850PID setPid05(String pid05) {
		this.pid05 = pid05;
		return this;
	}
	
	public PO850PID setItemDescription(String pid05) {
		this.pid05 = pid05;
		return this;
	}
	
}
