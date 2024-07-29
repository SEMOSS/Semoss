package prerna.reactor.qs.source;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

/*
 * Find a path to a given space.
 * This can be a user/insight/project space.
 * SpaceFinder ( space = "User", filePath="/" ) ;
 * "C:/workspace/Semoss/user/Asset__9742bc84-4793-49fb-b307-4685eaf46dc1/app_root/"
 * SpaceFinder ( space = "Insight" filePath="/" ) ;
 * "C:/workspace/Semoss/InsightCache/D5A21D14A23549D747696F1657C5B2C0/15fee6ea-1563-4dca-8ca9-658c95bb36cb/"
 * SpaceFinder ( space = "6d143c32-9a2e-415c-88ba-a739475bab3b" filePath="/images" ) ;
 * "C:/workspace/Semoss/project/Dragons__6d143c32-9a2e-415c-88ba-a739475bab3b/app_root/images"
 */

public class SpaceFinderReactor extends AbstractReactor {
    
    public SpaceFinderReactor() {
        this.keysToGet = new String[] {ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.FILE_PATH.getKey()};
    }
    
    
    @Override
    public NounMetadata execute() {
        organizeKeys();
        String fileLocation = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
        return new NounMetadata(fileLocation, PixelDataType.CONST_STRING);
    }
}
