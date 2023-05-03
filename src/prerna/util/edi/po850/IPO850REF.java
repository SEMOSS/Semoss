package prerna.util.edi.po850;

import prerna.util.edi.IX12Format;

public interface IPO850REF extends IX12Format {

	/**
	 * @return
	 */
	String getRef01();

	/**
	 * @param ref01
	 * @return
	 */
	IPO850REF setRef01(String ref01);

	/**
	 * @param ref01
	 * @return
	 */
	IPO850REF setReferenceIdQualifier(String ref01);

	/**
	 * @return
	 */
	String getRef02();

	/**
	 * @param ref02
	 * @return
	 */
	IPO850REF setRef02(String ref02);

	/**
	 * @param ref02
	 * @return
	 */
	IPO850REF setReferenceId(String ref02);

}