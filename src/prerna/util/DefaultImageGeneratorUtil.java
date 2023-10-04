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
		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace("\\", "/");
		if(!baseDirectory.endsWith("/")) {
			baseDirectory = baseDirectory + "/";
		}
		String imageDir = baseDirectory + "images" + File.separator + "stock-engines";
		File f = new File(imageDir);
		String[] a = f.list();
		Random rand = new Random();

		int i = rand.nextInt(a.length);
		String newImage = a[i];
		
		File thisNewImage = new File(imageDir + File.separator + newImage);
		// make the file location directory if it doesn't already exist
		{
			File fileDir = new File(fileLocation).getParentFile();
			if(!fileDir.exists() || !fileDir.isDirectory()) {
				fileDir.mkdirs();
			}
		}
		Path p = thisNewImage.toPath();
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
