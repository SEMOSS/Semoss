package prerna.util.edi;

public interface IPO850CTT extends IX12Format {

	/**
	 * @return
	 */
	int getCtt01();

	/**
	 * @param ctt01
	 * @return
	 */
	IPO850CTT setCtt01(int ctt01);

	/**
	 * @param ctt01
	 * @return
	 */
	IPO850CTT setNumPO1Segments(int ctt01);

	/**
	 * @return
	 */
	int getCtt02();

	/**
	 * @param ctt02
	 * @return
	 */
	IPO850CTT setCtt02(int ctt02);

	/**
	 * @param ctt02
	 * @return
	 */
	IPO850CTT setSumPO102Qualities(int ctt02);
	
	
}