package prerna.util.edi;

public interface IPO850IEA extends IX12Format {

	/**
	 * 
	 * @return
	 */
	String getIea01();

	/**
	 * 
	 * @param iea01
	 * @return
	 */
	IPO850IEA setIea01(String iea01);
	
	/**
	 * 
	 * @param iea01
	 * @return
	 */
	IPO850IEA setTotalGroups(String iea01);

	/**
	 * 
	 * @return
	 */
	String getIea02();

	/**
	 * 
	 * @param iea02
	 * @return
	 */
	IPO850IEA setIea02(String iea02);

	/**
	 * 
	 * @param iea02
	 * @return
	 */
	IPO850IEA setInterchangeControlNumber(String iea02);
	
}
