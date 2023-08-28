package prerna.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DefaultImageGeneratorUtil {
	
	private static final Logger classLogger = LogManager.getLogger(DefaultImageGeneratorUtil.class);

	/**
	 * Picks a random image for an engine
	 * @param fileLocation
	 * @return
	 */
	public static File pickRandomImage(String fileLocation) {
		String image = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String imageDir = image + File.separator + "images" + File.separator + "stock-engines";
		File f = new File(imageDir);
		String[] a = f.list();
		Random rand = new Random();

		int i = rand.nextInt(a.length);
		String newImage = a[i];
		
		File def = new File(imageDir + File.separator + newImage);
		
		Path p = def.toPath();
		Path from = Paths.get(fileLocation);
		try {
			Files.copy(p, Files.newOutputStream(from));
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		f = new File(fileLocation);
		return f;
	}

}
