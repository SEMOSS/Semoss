package prerna.comments;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.filefilter.WildcardFileFilter;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsightCommentHelper {

	private static final String ILLEGAL_ARGUMENT_MESSAGE = "Tampered insight comments. Please undo all comment changes and resync";
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();


	private InsightCommentHelper() {

	}

	/**
	 * @param engineName
	 * @param rdbmsId
	 * @return
	 */
	public static LinkedList<InsightComment> generateInsightCommentList(String engineName, String rdbmsId) {
		LinkedList<InsightComment> commentList = new LinkedList<>();

		// keep a reference to the first comment
		// so we can properly construct the ordering
		InsightComment firstComment = null;

		String baseDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR 
				+ Constants.PROJECT_FOLDER + DIR_SEPARATOR
				+ engineName + DIR_SEPARATOR + "version" + DIR_SEPARATOR + rdbmsId;

		// find all the comment files in the directory
		File dir = new File(Utility.normalizePath(baseDir));
		List<String> accept = new ArrayList<>();
		accept.add("*" + InsightComment.COMMENT_EXTENSION);
		FilenameFilter filter = new WildcardFileFilter(accept);
		File[] commentFiles = dir.listFiles(filter);

		// well, i guess no comments for you...
		if (commentFiles == null || commentFiles.length == 0) {
			return commentList;
		}

		// keep a map of the comment id
		// to the insight comment object
		Map<String, InsightComment> idToCommentHash = new HashMap<>();
		// now, we need to loop through and order them
		for (File f : commentFiles) {
			InsightComment c = InsightComment.loadFromFile(f);
			idToCommentHash.put(c.getId(), c);

			// test if it is the first one
			if (c.getPrevId() == null) {
				if (firstComment != null) {
					// someone has been naughty!
					throw new IllegalArgumentException(ILLEGAL_ARGUMENT_MESSAGE);
				}
				firstComment = c;
			}
		}

		// now, we construct the list
		commentList.add(firstComment);
		String nextId = null;

		if (firstComment != null) {
			nextId = firstComment.getNextId();
		}

		while (nextId != null) {
			InsightComment nextComment = idToCommentHash.get(nextId);
			if (nextComment == null) {
				// naughty again ... you are missing a comment
				throw new IllegalArgumentException(ILLEGAL_ARGUMENT_MESSAGE);
			}
			commentList.add(nextComment);
			nextId = nextComment.getNextId();
		}

		// lastly, check the size
		if (idToCommentHash.size() != commentList.size()) {
			// naughty again ... your chain is not accurate
			throw new IllegalArgumentException(ILLEGAL_ARGUMENT_MESSAGE);
		}

		return commentList;
	}

	public static void addInsightCommentToList(LinkedList<InsightComment> commentList, InsightComment newComment) {
		int size = commentList.size();
		if (size == 0) {
			// we have the first comment
			newComment.writeToFile();
			commentList.add(newComment);
		} else {
			// rework the references
			InsightComment lastComment = commentList.get(size - 1);
			lastComment.setNextId(newComment.getId());
			newComment.setPrevId(lastComment.getId());
			// and then write them
			lastComment.writeToFile();
			newComment.writeToFile();

			// add it to the list
			commentList.add(newComment);
		}
	}
}
