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

	@SuppressWarnings("restriction")
	/**
	 * 
	 * @param url
	 * @param imageName
	 *            add png extension
	 */
	public void showWindow(String url, String imagePath) throws IOException {
		// JavaFX stuff needs to be done on JavaFX thread
		Platform.setImplicitExit(false);
		Platform.runLater(new Runnable() {
			private Stage window;

			@SuppressWarnings("restriction")
			@Override
			public void run() {

				Stage window = new Stage();
				window.setTitle(url);

				browser = new Browser(url);

				monitorPageStatus(imagePath, window);

				VBox layout = new VBox();
				layout.getChildren().addAll(browser);
				Scene scene = new Scene(layout);
				window.setScene(scene);
				window.setOnCloseRequest(we -> System.exit(0));
				window.show();
			}
		});
	}

	private void monitorPageStatus(String imageName, Stage window) {

		timer.schedule(new TimerTask() {
			@SuppressWarnings("restriction")
			public void run() {
				Platform.runLater(() -> {
					if (browser.isPageLoaded()) {
						System.out.println("Page now loaded, taking screenshot...");
						saveAsPng(imageName);
						window.close();
						cancel();
						complete = true;
					} else
						System.out.println("Loading page...");
				});
			}
		}, 1000, 1000);
	}

	private void saveAsPng(String imageName) {
		WritableImage image = browser.snapshot(new SnapshotParameters(), null);
		// TODO change file path?
		File file = new File(imageName);
		try {
			ImageIO.write(SwingFXUtils.fromFXImage(image, null), "png", file);
			System.out.println("Screenshot saved as " + imageName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean getComplete() {
		boolean complete = false;
		while (!complete) {
			if (this.complete) {
				complete = true;
			} else {
				System.out.println("waiting");
			}

		}
		return complete;
	}

	public static void main(String[] args) throws IOException {
		System.out.println("Taking photo start");

		InsightScreenshot pic = new InsightScreenshot();
		System.out.println("Taking photo...");
		pic.showWindow("http://localhost:8080/SemossWeb/embed/#/embed?engine=movie&questionId=1&settings=false",
				"C:\\Users\\rramirezjimenez\\workspace\\Semoss\\images\\insight1.png");
		pic.getComplete();
		String serialized_image = InsightScreenshot.imageToString("C:\\Users\\rramirezjimenez\\workspace\\Semoss\\images\\insight1.png");	
		System.out.println(serialized_image);
		System.out.println("Final End");
		
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

	public static String imageToString(String path) {

		RenderedImage bi;
		String base64String = "";
		try {
			bi = javax.imageio.ImageIO.read(new File(path));
			base64String = imgToBase64String(bi, "png");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return "data:image/png;base64," + base64String;
	}

}
