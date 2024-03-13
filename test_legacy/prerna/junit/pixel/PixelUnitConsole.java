package prerna.junit.pixel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import prerna.sablecc2.PixelRunner;

public class PixelUnitConsole extends PixelUnit {

	@Test
	public void runConsole() {
		String end = "";
		while(!end.equalsIgnoreCase("end")) {
			try {
				
				// Initialize the "test" - setup state
				initializeTest(false);

				// Read pixel from tester
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
								
				classLogger.info("Enter Pixel command (separated by ; and on one line): ");
				String pixel = reader.readLine();   
				pixel = pixel.trim();
				
				// Run the pixel
				if(!pixel.isEmpty()) {
					PixelRunner returnData = runPixel(pixel);
					JsonArray allPixelReturns = getPixelReturns(returnData);
					JsonElement lastPixelReturn = allPixelReturns.get(allPixelReturns.size() - 1);
					
					classLogger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
					classLogger.info("ALL: ");
					System.out.println(GSON_PRETTY.toJson(allPixelReturns));
					classLogger.info("LAST: ");
					System.out.println(GSON_PRETTY.toJson(lastPixelReturn));
					classLogger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				} else {
					
					// Otherwise terminate if the tester enters without typing a Pixel
					end = "end";
				}
			} catch(Exception e) {
				classLogger.error("Error: ", e);
			} finally {
				
				// Destroy the "test" - reset state
				destroyTest();
			}
		}
	}
	
}