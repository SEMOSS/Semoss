//http://stackoverflow.com/questions/6112419/handling-the-tab-character-in-java
//http://stackoverflow.com/questions/10250617/java-apache-poi-can-i-get-clean-text-from-ms-word-doc-files
package prerna.poi.main;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.xml.sax.ContentHandler;

class TextExtractor { 
    private OutputStream outputstream;
    private ParseContext context;
    private Detector detector;
    private Parser parser;
    private Metadata metadata;
    private String extractedText;

    public TextExtractor() {
        context = new ParseContext();
        detector = new DefaultDetector();
        parser = new AutoDetectParser(detector);
        context.set(Parser.class, parser);
        outputstream = new ByteArrayOutputStream();
        metadata = new Metadata();
    }

    public void process(String filename) throws Exception {
        URL url;
        File file = new File(filename);
        if (file.isFile()) {
            url = file.toURI().toURL();
        } else {
            url = new URL(filename);
        }
        InputStream input = TikaInputStream.get(url, metadata);
        ContentHandler handler = new BodyContentHandler(outputstream);
        parser.parse(input, handler, metadata, context); 
        input.close();
    }

    public String getString() throws IOException {
        //Get the text into a String object
        extractedText = outputstream.toString();
        extractedText = extractedText.replace("\n", " ").replace("\r", " "); //MUST COME BACK
        //Do whatever you want with this String object.
        System.out.println("Extractedtext "+extractedText);
  //      String docname = "PKrequest\\PKuseCase.txt";
	//	System.out.println(docname);
	//	FileOutputStream out = new FileOutputStream(docname);
	//	extractedText = extractedText.replace("\t",".");
	//	out.write(extractedText.getBytes());
	//	out.close();
        
		return extractedText;
    }
    
    public String WebsiteTextExtractor(String docin) throws Exception{
		
    	final String url = docin;
    	boolean knownwebsite = false;
		org.jsoup.nodes.Document doc = Jsoup.connect(url).get();
		String extractedtext = "";
		
		if(url.contains("nytimes.com")){
		knownwebsite = true;
			for( Element element : doc.select("p.story-body-text") )
		{
		    if( element.hasText() ) // Skip those tags without text
		    {
		        System.out.println("This is element text"+element.text());
		        extractedtext = extractedtext.concat(element.text().toString());
		    }
		}
		}
		if(!knownwebsite){
			extractedtext = doc.text();
		}
		extractedtext = extractedtext.replace("\n", " @ ").replace("\r", " ");
		System.out.println("extracted text being sent back"+extractedtext);
    	return extractedtext;
    	
    }

    public String WorddocTextExtractor(String docin) throws Exception{
    	
    	TextExtractor textExtractor = new TextExtractor();
         textExtractor.process(docin);
        String extractedText = textExtractor.getString();
        return extractedText;
    	
    }
    
    public String TextDocExtractor(String docin) throws Exception{
    	
    	TextExtractor textExtractor = new TextExtractor();
         textExtractor.process(docin);
        String extractedText = textExtractor.getString();
        return extractedText;
    	
    }

	public String MasterResumeExtractor(String docin) throws Exception{
    	TextExtractor textExtractor = new TextExtractor();
        textExtractor.process(docin);
       String extractedText = textExtractor.getString();
       
       return extractedText;
   	
    }
   
}