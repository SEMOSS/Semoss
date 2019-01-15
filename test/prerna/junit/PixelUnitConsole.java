package prerna.junit;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import prerna.sablecc2.PixelRunner;

public class PixelUnitConsole extends PixelUnitTest {

	@Test
	public void runConsole() {
		String end = "";
		while(!end.equalsIgnoreCase("end")) {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				LOGGER.info("Enter Pixel command: ");
				String pixel = reader.readLine();   
				pixel = pixel.trim();
				if(!pixel.isEmpty()) {
					PixelRunner returnData = runPixel(pixel);
					String actualJson = GSON.toJson(returnData.getResults());
					LOGGER.info(actualJson);
				} else {
					end = "end";
				}
			} catch(Exception e) {
				LOGGER.error(e);
			}
		}
	}
	
}
