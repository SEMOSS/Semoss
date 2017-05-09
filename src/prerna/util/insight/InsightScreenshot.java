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

//import org.apache.commons.net.util.Base64;
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
	public Stage stage;
	private Timer timer = new java.util.Timer();
	private boolean complete = false;

	/**
	 * Loads the url to the javafx browser
	 * 
	 * @param url
	 * @param imagePath
	 *            add png extension
	 */
	public void showUrl(String url, String imagePath) {
		// JavaFX stuff needs to be done on JavaFX thread
		Platform.setImplicitExit(false);
		Platform.runLater(new Runnable() {
			private Stage window;

			@SuppressWarnings("restriction")
			@Override
			public void run() {

				Stage window = new Stage();
				window.setTitle(url);

				// load url to broswer and wait til visualization is loaded
				browser = new Browser(url);
				monitorPageStatus(imagePath, window);

				VBox layout = new VBox();
				layout.getChildren().addAll(browser);
				Scene scene = new Scene(layout);
				window.setScene(scene);
				window.setOnCloseRequest(we -> System.exit(0));
				// window.setOpacity(0);
				window.show();
			}
		});
	}

	private void monitorPageStatus(String imagePath, Stage window) {

		timer.schedule(new TimerTask() {
			@SuppressWarnings("restriction")
			public void run() {
				Platform.runLater(() -> {
					if (browser.isPageLoaded()) {
						// System.out.println("Page now loaded, taking
						// screenshot...");
						saveAsPng(imagePath);
						window.close();
						cancel();
						complete = true;
					}
				});
			}
		}, 1000, 1000);
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
			// System.out.println("Screenshot saved as " + imagePath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Checks if the browser has taken a screenshot
	 * 
	 * @return
	 */
	public boolean getComplete() {
		boolean complete = false;
		while (!complete) {
			if (this.complete) {
				complete = true;
			}

//			System.out.println("waiting");

		}
		return complete;
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Taking photo start");

		InsightScreenshot pic = new InsightScreenshot();
		System.out.println("Taking photo...");
		pic.showUrl("http://localhost:8080/SemossWebBranch/embed/#/embed?engine=movieMay5&questionId=18&settings=false",
				"C:\\workspace\\Semoss\\images\\insight1.png");
		pic.getComplete();
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
	 */
	public static String imageToString(String path) {

		RenderedImage bi;
		String base64String = "";
		try {
			File imageFile = new File(path);
			bi = javax.imageio.ImageIO.read(imageFile);
			base64String = imgToBase64String(bi, "png");
		} catch (IOException e) {
			e.printStackTrace();
		}

		return base64String;
	}

}
