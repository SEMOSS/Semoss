package prerna.util.edi.impl.ghx.po850.writer.loop.po1loop;

import java.util.ArrayList;
import java.util.List;

import prerna.util.edi.IX12Format;
import prerna.util.edi.po850.enums.PO850PO1QualifierIdEnum;

public class PO850PO1 implements IX12Format {

	private String po101 = ""; // unique id within loop
	private int po102 = -1; // quantity ordered
	private String po103 = ""; // unit of basis for measurement CA=case, EA=each, etc.
	private double po104 = -1; // unit price
	private Double po105 = null; // basis of unit price PE
	
	// adding a list of qualifier to values
	private List<PO850PO1QualifierAndVal> qualAndVals = new ArrayList<>();
	
	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		if(po102 < 0) {
			throw new IllegalArgumentException("PO102 cannot be negative");
		}
		if(po104 < 0) {
			throw new IllegalArgumentException("PO104 cannot be negative");
		}
		String builder = "PO1" 
				+ elementDelimiter + po101
				+ elementDelimiter + po102
				+ elementDelimiter + po103
				+ elementDelimiter + po104
				+ elementDelimiter + ((po105 == null) ? "" : po105)
				;
		for(PO850PO1QualifierAndVal qualAndVal : qualAndVals) {
			builder = builder + elementDelimiter + qualAndVal.getQualifier()
					+ elementDelimiter + qualAndVal.getValue();
		}

		builder = builder + segmentDelimiter;
		return builder;
	}

	// setters/getter
	
	public String getPo101() {
		return po101;
	}

	public PO850PO1 setPo101(String po101) {
		this.po101 = po101;
		return this;
	}
	
	public PO850PO1 setUniqueId(String po101) {
		return setPo101(po101);
	}

	public int getPo102() {
		return po102;
	}

	public PO850PO1 setPo102(int po102) {
		this.po102 = po102;
		return this;
	}
	
	public PO850PO1 setQuantityOrdered(int po102) {
		return setPo102(po102);
	}

	public String getPo103() {
		return po103;
	}

	public PO850PO1 setPo103(String po103) {
		this.po103 = po103;
		return this;
	}
	
	public PO850PO1 setUnitOfMeasure(String po103) {
		return setPo103(po103);
	}

	public double getPo104() {
		return po104;
	}

	public PO850PO1 setPo104(double po104) {
		this.po104 = po104;
		return this;
	}
	
	public PO850PO1 setUnitPrice(double po104) {
		return setPo104(po104);
	}

//	public String getPo105() {
//		return po105;
//	}
//
//	public PO850PO1 setPo105(String po105) {
//		this.po105 = po105;
//		return this;
//	}

	public PO850PO1 addQualifierAndValue(PO850PO1QualifierIdEnum qualifier, String value) {
		qualAndVals.add(new PO850PO1QualifierAndVal(qualifier.getId(), value));
		return this;
	}
	
	public String getPo1Val(int index) {
		if(index < 6) {
			throw new IllegalArgumentException("Index must be larger than 6");
		}
		
		int startIndex = index-6;
		for(PO850PO1QualifierAndVal qualAndVal : qualAndVals) {
			if(startIndex == 0) {
				return qualAndVal.getQualifier();
				
			}
			if(startIndex == 1) {
				return qualAndVal.getValue(); 
			}
			startIndex = startIndex-2;
		}
		
		String errorMessage = "Could not find PO1";
		if(index < 10) {
			errorMessage += "0"+index;
		} else {
			errorMessage += index;
		}
		throw new IllegalArgumentException(errorMessage);
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		PO850PO1 po1 = new PO850PO1()
			.setUniqueId("1") // 1 - unique id 
			.setQuantityOrdered(10) // 2 - quantity ordered
			.setUnitOfMeasure("BX") // 3 - unit measurement
			.setUnitPrice(27.50) // 4 unit price (not total)
			.addQualifierAndValue(PO850PO1QualifierIdEnum.VC, "BXTS1040")
			.addQualifierAndValue(PO850PO1QualifierIdEnum.IN, "299176")
			;
		
		System.out.println(po1.generateX12("^", "~\n"));
	}
	
	
}
