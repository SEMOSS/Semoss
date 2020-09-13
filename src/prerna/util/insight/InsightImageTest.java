package prerna.util.insight;

import java.io.IOException;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.layout.StackPane;
import javafx.scene.web.WebEngine;
import javafx.scene.web.WebView;
import javafx.stage.Stage;

public class InsightImageTest extends Application {
    @Override
    public void start(Stage stage) throws Exception {
    	//TODO change this to test your insight
    	String insightURL = "http://localhost:8080/SemossWeb/#!/insight?type=single&engine=movie&id=10&panel=0";
        StackPane pane = new StackPane();
        WebView view = new WebView();

        WebEngine engine = view.getEngine();
        engine.load(insightURL);
        pane.getChildren().add(view);

        Scene scene = new Scene(pane, 960, 600);
        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) throws IOException {
        Application.launch(args);
    }
}
