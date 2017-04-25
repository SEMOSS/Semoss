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
import java.io.File;
import java.util.Timer;
import java.util.TimerTask;
import javafx.scene.image.WritableImage;
import javafx.scene.SnapshotParameters;
import javax.imageio.ImageIO;
import java.io.IOException;

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

	@SuppressWarnings("restriction")
	/**
	 * 
	 * @param url
	 * @param imageName add png extension
	 */
	public void showWindow(String url, String imagePath) {
		// JavaFX stuff needs to be done on JavaFX thread
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
					} else
						System.out.println("Loading page...");
				});
			}
		}, 1000, 1000);
	}

	private void saveAsPng(String imageName) {
		WritableImage image = browser.snapshot(new SnapshotParameters(), null);
		//TODO change file path?
		File file = new File(imageName);
		try {
			ImageIO.write(SwingFXUtils.fromFXImage(image, null), "png", file);
			System.out.println("Screenshot saved as " + imageName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		InsightScreenshot pic = new InsightScreenshot();
		pic.showWindow("https://www.google.com", "images/google.png");
	
		InsightScreenshot pic2 = new InsightScreenshot();
		pic2.showWindow("https://www.facebook.com", "images/facebook.png");
	}
}
