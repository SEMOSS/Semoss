package prerna.junit;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import prerna.sablecc2.PixelRunner;

public class PixelUnitConsole extends PixelUnitWithDatabases {

	@Test
	public void runConsole() {
		String end = "";
		while(!end.equalsIgnoreCase("end")) {
			try {
				
				// Initialize the "test" - setup state
				initializeTest();

				// Read pixel from tester
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				LOGGER.info("Enter Pixel command (separated by ; and on one line): ");
				String pixel = reader.readLine();   
				pixel = pixel.trim();
				
				// Run the pixel
				if(!pixel.isEmpty()) {
					PixelRunner returnData = runPixel(pixel);
					String json = collectPixelJson(returnData);
					LOGGER.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
					LOGGER.info("OUTPUT: ");
					System.out.println(json);
					LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				} else {
					
					// Otherwise terminate if the tester enters without typing a Pixel
					end = "end";
				}
			} catch(Exception e) {
				LOGGER.error("Error: ", e);
			} finally {
				
				// Destroy the "test" - reset state
				destroyTest();
			}
		}
	}
	
}