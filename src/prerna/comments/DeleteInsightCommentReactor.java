package prerna.comments;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class DeleteInsightCommentReactor extends AbstractReactor {

	public DeleteInsightCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.INSIGHT_ID.getKey(),
				ReactorKeysEnum.COMMENT_ID_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
//		organizeKeys();
//		String engine = this.keyValue.get(this.keysToGet[0]);
//		if(engine == null) {
//			throw new IllegalArgumentException("Need to define which engine this insight belongs to");
//		}
//		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
//		if(rdbmsId == null) {
//			throw new IllegalArgumentException("Need to define which insight this comment belongs to");
//		}
//		String commentId = this.keyValue.get(this.keysToGet[2]);
//		if(commentId == null || commentId.trim().isEmpty()) {
//			throw new IllegalArgumentException("Need to define the comment id");
//		}
//		
//		// find the files relating to this id
//		// and delete them
//		String baseDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) 
//				+ "\\" + Constants.DB + "\\" + engine + "\\version\\" + rdbmsId;		
//		
//		File dir = new File(baseDir);
//		List<String> accept = new ArrayList<String>();
//		accept.add(commentId + "*");
//		FilenameFilter filter = new WildcardFileFilter(accept);
//		File[] filesToDelete = dir.listFiles(filter);
//		if(filesToDelete.length > 0) {
//			for(File fDelete : filesToDelete) {
//				fDelete.delete();
//			}
//			return new NounMetadata(true, PixelDataType.BOOLEAN);
//		} else {
//			return new NounMetadata(false, PixelDataType.BOOLEAN);
//		}
		return null;
	}
	
}