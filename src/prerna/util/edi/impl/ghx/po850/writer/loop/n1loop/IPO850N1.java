package prerna.util.edi.impl.ghx.po850.writer.loop.n1loop;

import prerna.util.edi.IX12Format;

public interface IPO850N1 extends IX12Format {

	String getN101();

	PO850N1 setN101(String n101);

	PO850N1 setEntityCode(String n101);

	String getN102();

	PO850N1 setN102(String n102);

	PO850N1 setName(String n102);

	String getN103();

	PO850N1 setN103(String n103);

	PO850N1 setIdentificationCodeQualifier(String n103);

	String getN104();

	PO850N1 setN104(String n104);

	PO850N1 setIdentificationCode(String n104);

}