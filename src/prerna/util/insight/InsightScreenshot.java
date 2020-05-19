//package prerna.util.insight;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Timer;
//import java.util.TimerTask;
//
//import javax.imageio.ImageIO;
//
//import org.apache.commons.codec.binary.Base64;
//import org.apache.log4j.Logger;
//
//import javafx.application.Platform;
//import javafx.embed.swing.JFXPanel;
//import javafx.embed.swing.SwingFXUtils;
//import javafx.scene.Scene;
//import javafx.scene.SnapshotParameters;
//import javafx.scene.image.WritableImage;
//import javafx.scene.layout.VBox;
//import javafx.stage.Stage;
//
///**
// * @author ericjbruno
// */
//public class InsightScreenshot {
//	{
//		// Clever way to init JavaFX once
//		JFXPanel fxPanel = new JFXPanel();
//	}
//	private Browser browser;
//	private Timer timer = new java.util.Timer();
//	private boolean complete = false;
//	private boolean validStage = false;
//	private Stage window;
//	static Logger LOGGER = Logger.getLogger(InsightScreenshot.class.getName());
//	private String url;
//
//	/**
//	 * Loads the url to the javafx browser
//	 * 
//	 * @param url
//	 * @param imagePath
//	 *            add png extension
//	 */
//	@SuppressWarnings("restriction")
//	public void showUrl(String url2, String imagePath) {
//		// JavaFX stuff needs to be done on JavaFX thread
//		Platform.setImplicitExit(false);
//		Platform.runLater(new Runnable() {
//
//			@SuppressWarnings("restriction")
//			@Override
//			public void run() {
//				window = new Stage();
//				url = url2;
//				window.setTitle(url);
//
//				// load url to broswer and wait til visualization is loaded
//				browser = new Browser(url, window);
//				monitorPageStatus(imagePath, window);
//
//				VBox layout = new VBox();
//				layout.getChildren().addAll(browser);
//				Scene scene = new Scene(layout);
//				window.setScene(scene);
//				window.setOpacity(0);
//				window.show();
//				validStage = true;
//			}
//		});
//	}
//
//	private void monitorPageStatus(String imagePath, Stage window) {
//		timer.schedule(new TimerTask() {
//			@SuppressWarnings("restriction")
//			public void run() {
//				Platform.runLater(() -> {
//					if (browser.isPageLoaded()) {
//						saveAsPng(imagePath);
//						window.close();
//						complete = true;
//						cancel();
//					}
//				});
//			}
//		}, 0, 5000);
//
//	}
//
//	/**
//	 * Take a screenshot of the browser and save as png
//	 * 
//	 * @param imagePath
//	 */
//	private boolean saveAsPng(String imagePath) {
//		WritableImage image = browser.snapshot(new SnapshotParameters(), null);
//		File file = new File(imagePath);
//		boolean imageTaken = false;
//		try {
//			long startTime = file.lastModified();
//			imageTaken = ImageIO.write(SwingFXUtils.fromFXImage(image, null), "png", file);
//			// wait for output file to print
//			if (imageTaken) {
//				boolean newFile = false;
//				while (!newFile) {
//					File imageFile = new File(imagePath);
//					long endTime = imageFile.lastModified();
//					if (endTime > startTime) {
//						// System.out.println("SAVED IMAGE TO FILE");
//						newFile = true;
//					}
//				}
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		return imageTaken;
//	}
//
//	/**
//	 * Checks if the browser has taken a screenshot
//	 * 
//	 * @return
//	 * @throws Exception
//	 *             if browser is unable to capture image
//	 */
//	@SuppressWarnings("restriction")
//	public boolean getComplete() throws Exception {
//		boolean complete = false;
//		int count = 0;
//		int secondDelay = 0;
//		while (!complete) {
//			if (this.complete) {
//				complete = true;
//			}
//			long millis = System.currentTimeMillis();
//			try {
//				Thread.sleep(1000 - millis % 1000);
//				secondDelay++;
//				// print every 30 seconds if image thread is active
//				if (secondDelay % 30 == 0) {
//					LOGGER.info("saving insight image in progress...");
//				}
//				if (validStage) {
//					// TODO change this threshold to wait on image capture
//					if (count == 180) {
//						Platform.runLater(() -> {
//							window.close();
//						});
//						complete = true;
//						timer.cancel();
//						throw new Exception();
//					}
//
//					count++;
//				}
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//		}
//		return complete;
//	}
//
//	public static void main(String[] args) throws IOException {
//		// System.out.println("Taking photo start");
//		//
//		// InsightScreenshot pic = new InsightScreenshot();
//		// System.out.println("Taking photo...");
//		// pic.showUrl("http://localhost:8080/SemossWeb/embed/#/embed?engine=movie&questionId=80&settings=false",
//		// "C:\\workspace\\Semoss\\images\\insight1.png");
//		// try {
//		// pic.getComplete();
//		// } catch (Exception e) {
//		// e.printStackTrace();
//		// }
//		// String serialized_image =
//		// InsightScreenshot.imageToString("C:\\workspace\\Semoss\\images\\insight1.png");
//		// System.out.println(serialized_image);
//		// System.exit(0);
//
//		String filePath = "C:\\workspace\\Semoss\\images\\h2movie_7.png";
//		Path path = Paths.get(filePath);
//		String base64Str = "";
//		byte[] codedFile = null;
//		try {
//			codedFile = Files.readAllBytes(path);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		base64Str = Base64.encodeBase64String(codedFile);
//		System.out.println(base64Str);
//	}
//
//	/**
//	 * Return the based 64 serialized string
//	 * 
//	 * @param path
//	 * @return
//	 * @throws IOException
//	 */
//	public static String imageToString(String filePath) throws IOException {
//		Path path = Paths.get(filePath);
//		String base64Str = "";
//		byte[] codedFile = null;
//		try {
//			codedFile = Files.readAllBytes(path);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		base64Str = Base64.encodeBase64String(codedFile);
//		return base64Str;
//	}
//
//}
