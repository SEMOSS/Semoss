package prerna.util.insight;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.embed.swing.JFXPanel;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.layout.StackPane;
import javafx.stage.Stage;
import prerna.util.insight.Browser;
import javafx.scene.layout.VBox;
import javafx.embed.swing.SwingFXUtils;

import java.awt.image.RenderedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Timer;
import java.util.TimerTask;
import javafx.scene.image.WritableImage;
import javafx.scene.SnapshotParameters;
import javax.imageio.ImageIO;
import org.apache.log4j.Logger;

import java.util.Base64;
import org.apache.commons.vfs2.FileNotFoundException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import javafx.concurrent.Task;

/**
 * @author ericjbruno
 */
public class InsightScreenshot {
	{
		// Clever way to init JavaFX once
		JFXPanel fxPanel = new JFXPanel();
	}
	private Browser browser;
	private Timer timer = new java.util.Timer();
	private boolean complete = false;
	private boolean validStage = false;
	private Stage window;
	Logger LOGGER = Logger.getLogger(InsightScreenshot.class.getName());
	private String url;

	/**
	 * Loads the url to the javafx browser
	 * 
	 * @param url
	 * @param imagePath
	 *            add png extension
	 */
	@SuppressWarnings("restriction")
	public void showUrl(String url2, String imagePath) {
		// JavaFX stuff needs to be done on JavaFX thread
		Platform.setImplicitExit(false);
		Platform.runLater(new Runnable() {

			@SuppressWarnings("restriction")
			@Override
			public void run() {
				window = new Stage();
				url = url2;
				window.setTitle(url);

				// load url to broswer and wait til visualization is loaded
				browser = new Browser(url);
				monitorPageStatus(imagePath, window);

				VBox layout = new VBox();
				layout.getChildren().addAll(browser);
				Scene scene = new Scene(layout);
				window.setScene(scene);
				window.setOnCloseRequest(we -> System.exit(0));
				window.setOpacity(0);
				window.show();
				validStage = true;
			}
		});
	}

	private void monitorPageStatus(String imagePath, Stage window) {
		timer.schedule(new TimerTask() {
			@SuppressWarnings("restriction")
			public void run() {
				Platform.runLater(() -> {
					if (browser.isPageLoaded()) {
						saveAsPng(imagePath);
						window.close();
						cancel();
						complete = true;
					}
				});
			}
		}, 0, 1000);

	}

	/**
	 * Take a screenshot of the browser and save as png
	 * 
	 * @param imagePath
	 */
	private void saveAsPng(String imagePath) {
		WritableImage image = browser.snapshot(new SnapshotParameters(), null);
		File file = new File(imagePath);
		try {
			ImageIO.write(SwingFXUtils.fromFXImage(image, null), "png", file);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Checks if the browser has taken a screenshot
	 * 
	 * @return
	 */
	@SuppressWarnings("restriction")
	public boolean getComplete() {
		boolean complete = false;
		int i = 0;
		int count = 0;
		while (!complete) {
			if (this.complete) {
				complete = true;
			}
			if (i % 1000000000 == 0) {
				LOGGER.info("saving insight image");
				if (validStage) {
					// TODO change this threshold to wait on image capture
					if (count == 180) {
						Platform.runLater(() -> {
							window.close();
						});
						LOGGER.info("Unable to capture image from " + url);
						complete = true;
						timer.cancel();
					}

					count++;
				}
			}

			i++;

		}
		return complete;
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Taking photo start");

		InsightScreenshot pic = new InsightScreenshot();
		System.out.println("Taking photo...");
		pic.showUrl("http://localhost:8080/SemossWeb/embed/#/embed?engine=movie&questionId=80&settings=false",
				"C:\\workspace\\Semoss\\images\\insight1.png");
		try {
			pic.getComplete();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String serialized_image = InsightScreenshot.imageToString("C:\\workspace\\Semoss\\images\\insight1.png");
		System.out.println(serialized_image);
		System.exit(0);

	}

	private static String imgToBase64String(final RenderedImage img, final String formatName) {
		final ByteArrayOutputStream os = new ByteArrayOutputStream();
		try {
			ImageIO.write(img, formatName, Base64.getEncoder().wrap(os));
			return os.toString(StandardCharsets.ISO_8859_1.name());
		} catch (final IOException ioe) {
			throw new UncheckedIOException(ioe);
		}
	}

	/**
	 * Return the based 64 serialized string
	 * 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static String imageToString(String path) throws IOException {

		RenderedImage bi;
		String base64String = "";

		File imageFile = new File(path);
		bi = javax.imageio.ImageIO.read(imageFile);
		base64String = imgToBase64String(bi, "png");

		return base64String;
	}

}
