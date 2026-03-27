package prerna.reactor.agent.mcp.tools;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListRoomFilesReactor extends AbstractReactor {

  @Override
  public NounMetadata execute() {
    Path roomPath = new File(insight.getInsightFolder()).toPath();

    List<String> fileList;
    try {
      fileList = RoomFileUtils.collectVisibleFiles(roomPath).stream()
          .map(p -> roomPath.relativize(p).toString())
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to list room files: " + e.getMessage());
    }

    return new NounMetadata(fileList, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Lists all files in the room recursively, including files in subdirectories. "
        + "Paths are relative to the room folder. Hidden directories and files (starting with '.') "
        + "are always excluded.";
  }
}