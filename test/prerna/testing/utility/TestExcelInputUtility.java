package prerna.testing.utility;

import java.time.LocalDateTime;

public class TestExcelInputUtility {

	public static TestExcelInputObject getString(String s) {
		TestExcelInputObject o = new TestExcelInputObject();
		o.setS(s);
		o.setType(TestExcelType.STRING);
		return o;
	}

	public static TestExcelInputObject getInteger(int i) {
		TestExcelInputObject o = new TestExcelInputObject();
		o.setI(i);
		o.setType(TestExcelType.INTEGER);
		return o;
	}

	public static TestExcelInputObject getDate(LocalDateTime ldt) {
		TestExcelInputObject o = new TestExcelInputObject();
		o.setLdt(ldt);
		o.setType(TestExcelType.DATE);
		return o;
	}

	public static TestExcelInputObject getDouble(double d) {
		TestExcelInputObject o = new TestExcelInputObject();
		o.setD(d);
		o.setType(TestExcelType.DOUBLE);
		return o;
	}

	public static TestExcelInputObject getBoolean(boolean b) {
		TestExcelInputObject o = new TestExcelInputObject();
		o.setB(b);
		o.setType(TestExcelType.BOOLEAN);
		return o;
	}

	public static TestExcelInputObject isNull(boolean isNull) {
		TestExcelInputObject o = new TestExcelInputObject();
		o.setNull(isNull);
		o.setType(TestExcelType.NULL);
		return o;
	}

	// public static TestExcelInputObject getString(String s) {
	// 	TestExcelInputObject o = new TestExcelInputObject();
	// 	o.setS(s);
	// 	o.setType(TestExcelType.STRING);
	// 	return o;
	// }
}
