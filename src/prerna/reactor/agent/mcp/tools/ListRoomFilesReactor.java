package prerna.reactor.agent.mcp.tools;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListRoomFilesReactor extends AbstractReactor {

  @Override
  public NounMetadata execute() {
    File roomFolder = new File(insight.getInsightFolder());

    List<String> fileArr =
        Arrays.stream(roomFolder.listFiles()).filter(File::isFile).map(File::getName).toList();

    return new NounMetadata(fileArr, PixelDataType.MAP);
  }

  @Override
  public String getReactorDescription() {
    return "Lists all files in the room - no directories. No parameters are required.";
  }
}