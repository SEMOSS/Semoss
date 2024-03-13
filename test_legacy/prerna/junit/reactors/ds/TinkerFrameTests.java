package prerna.junit.reactors.ds;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import prerna.ds.TinkerFrame;

public class TinkerFrameTests {

	@Test
	public void tinkerFrame() {
		TinkerFrame tf = new TinkerFrame();
		assertTrue(tf.getName().startsWith("TINKER_"));
	}
}
