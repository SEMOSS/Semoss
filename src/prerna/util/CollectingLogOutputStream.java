package prerna.util;

import java.util.LinkedList;
import java.util.List;
import org.apache.commons.exec.LogOutputStream;

public class CollectingLogOutputStream extends LogOutputStream {
    private final List<String> lines = new LinkedList<String>();
    @Override protected void processLine(String line, int level) {
        lines.add(line);
    }   
    public List<String> getLines() {
        return lines;
    }
}