package prerna.junit.reactors.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.io.connector.antivirus.clamav.ClamAVScannerUtils;
import xyz.capybara.clamav.Platform;

public class ClamAVScannerUtilsTests {
	
	// Might fail if not run on windows (I.E a Linux server)
	@Test
	public void testGetVirusScanningFileSystem() {
		Platform pf = ClamAVScannerUtils.getVirusScanningFileSystem();
		assertEquals(Platform.WINDOWS, pf);
	}

}
