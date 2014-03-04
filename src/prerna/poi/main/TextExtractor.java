//http://stackoverflow.com/questions/6112419/handling-the-tab-character-in-java
//http://stackoverflow.com/questions/10250617/java-apache-poi-can-i-get-clean-text-from-ms-word-doc-files
package prerna.poi.main;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URL; 
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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
import org.xml.sax.InputSource;

import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.BoilerpipeExtractor;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.extractors.CommonExtractors;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLDocument;
import de.l3s.boilerpipe.sax.HTMLFetcher;


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
        extractedText = extractedText.replace("\n", " @ ").replace("\r", " ");
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
		
		if(url.contains("nytimes.com") && false){
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
		if(knownwebsite){
			System.out.println("NON USED WEB READER");
			extractedtext = doc.text();
		}
		if(!knownwebsite){
			System.out.println("USED WEB READER");
			URL urlobj = new URL(url);
			// NOTE: Use ArticleExtractor unless DefaultExtractor gives better results for you   
		   String text = ArticleExtractor.INSTANCE.getText(urlobj);
		 	extractedtext = text;   
		   System.out.println(text);
					}
    //	}
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
    private String Resumeprocessing(String extractedText2) {
    	Scanner scan;
    	scan = new Scanner(extractedText2);
		return null;
	}

	public String MasterResumeExtractor(String docin) throws Exception{
    	TextExtractor textExtractor = new TextExtractor();
        textExtractor.process(docin);
       String extractedText = textExtractor.getString();
       
       return extractedText;
   	
    }
    public static void main(String[] args) throws Exception {
    //    if (args.length == 1) {
       //     TextExtractor textExtractor = new TextExtractor();
       //     textExtractor.process("PKrequest\\PKuseCase.docx");
       //     textExtractor.getString();
   //     } else { 
   //         throw new Exception();
   //     }
    	URL url = new URL("http://www.washingtonpost.com/world/currencies-of-russia-ukraine-fall-monday/2014/03/03/5f3af2c2-a2c9-11e3-a5fa-55f0c77bf39c_story.html?hpid=z1");  
 	   // NOTE: Use ArticleExtractor unless DefaultExtractor gives better results for you   
   String text = ArticleExtractor.INSTANCE.getText(url);
 	   System.out.println(text);
    }
}