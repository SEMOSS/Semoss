package prerna.reactor.agent.mcp.tools;

import prerna.util.files.SemossParsedFile;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns approximate token statistics for extracted room files. */
public class GetRoomFileTokenStatsReactor extends AbstractReactor {

  public GetRoomFileTokenStatsReactor() {
    this.keysToGet = new String[] {};
    this.keyRequired = new int[] {};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    Path roomPath = new File(insight.getInsightFolder()).toPath();

    List<Path> filePaths;
    try {
      filePaths = RoomFileUtils.collectVisibleFiles(roomPath);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to list room files: " + e.getMessage());
    }

    Map<String, Map<String, Object>> results = filePaths.parallelStream()
        .map(path -> {
          File file = path.toFile();
          String relativePath = roomPath.relativize(path).toString();
          Map<String, Object> fileResult = new HashMap<>();
          try {
            SemossParsedFile semossParsedFile = new SemossParsedFile(file);
            String extractedContent = semossParsedFile.getExtractedContents();
            if (extractedContent == null) {
              fileResult.put("status", "no_content");
            } else {
              int charCount = extractedContent.length();
              int wordCount = countWords(extractedContent);
              int approxTokens = (int) Math.ceil(charCount / 4.0);
              fileResult.put("status", "ok");
              fileResult.put("extractedPath", semossParsedFile.getExtractedContentsFilePath());
              fileResult.put("charCount", charCount);
              fileResult.put("wordCount", wordCount);
              fileResult.put("approxTokens", approxTokens);
            }
          } catch (IOException e) {
            fileResult.put("status", "error");
            fileResult.put("message", e.getMessage());
          }
          return new AbstractMap.SimpleEntry<>(relativePath, fileResult);
        })
        .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));

    return new NounMetadata(results, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Returns approximate token counts for extracted room files (char/word counts +"
        + " approxTokens).";
  }

  private int countWords(String content) {
    int count = 0;
    boolean inWord = false;
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (Character.isWhitespace(c)) {
        inWord = false;
      } else if (!inWord) {
        count++;
        inWord = true;
      }
    }
    return count;
  }
}