package prerna.engine.impl.function;

import com.microsoft.cognitiveservices.speech.audio.PullAudioInputStreamCallback;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class BinaryAudioStreamReader extends PullAudioInputStreamCallback {

 InputStream inputStream;

 BinaryAudioStreamReader(String fileName) throws FileNotFoundException {
     File file = new File(fileName);
     inputStream = new FileInputStream(file);
 }

 @Override
 public int read(byte[] dataBuffer) {
     try {
         return inputStream.read(dataBuffer, 0, dataBuffer.length);
     } catch (IOException e) {
         e.printStackTrace();
     }
     return 0;
 }

 @Override
 public void close() {
     try {
         inputStream.close();
     } catch (IOException e) {
         e.printStackTrace();
     }
 }
}