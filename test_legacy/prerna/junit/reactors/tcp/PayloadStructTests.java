package prerna.junit.reactors.tcp;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.tcp.PayloadStruct;

public class PayloadStructTests {

	@Test
	public void testPayloadStructTestsInit() {
		PayloadStruct ps = new PayloadStruct();
		assertEquals(PayloadStruct.OPERATION.R, ps.operation);
	}
}
