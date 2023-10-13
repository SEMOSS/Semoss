package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;
import prerna.util.Constants;

public class CSVWriter {

	private static final Logger classLogger = LogManager.getLogger(CSVWriter.class);
	
	// takes an input file
	// starts appending CSV to it
	String fileName = null;
	int contentLength = 512;
	int overlapLength = 10;
	float tokenLimit = 0.1f;
	PrintWriter pw = null;
	int minContentLength = 30;
	FileWriter fw = null;

	public CSVWriter(String fileName) {
		this.fileName = fileName;
		File file = new File(fileName);

		try {
			if(file.exists())
			{
				// no need to write headers
				// open in append mode
				fw = new FileWriter(file, true);
				pw = new PrintWriter(fw);
			}
			else
			{
				fw = new FileWriter(file, false);
				pw = new PrintWriter(fw);
				writeHeader();
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public void setTokenLength(int tokenLength)
	{
		this.contentLength = tokenLength;
	}

	public void overlapLength(int overlap) {
		this.overlapLength = overlap;
	}

	private void writeHeader() {
		StringBuffer row = new StringBuffer();
		row.append("Source").append(",")
		.append("Divider").append(",")
		.append("Part").append(",")
		.append("Content")
		.append("\r\n");
		pw.print(row + "");
	}

	/**
	 * divider is page number or slide number etc. 
	 * @param source
	 * @param divider
	 * @param content
	 * @param misc
	 */
	public void writeRow(String source, String divider, String content, String misc)
	{
		// tries to see if text is > token length
		// uses spacy to break this
		// gets the parts and then
		// takes this row and writes it
		List <String> contentBlocks = breakSentenceToBlocks(content);
		for(int contentIndex = 0;contentIndex < contentBlocks.size();contentIndex++)
		{
			String thisBlock = contentBlocks.get(contentIndex);
			if(thisBlock.length() >= minContentLength)
			{
				//System.err.println(contentIndex + " <> " + contentBlocks.get(contentIndex));
				StringBuilder row = new StringBuilder();
				row.append("\"").append(cleanString(source)).append("\"").append(",")
				.append("\"").append(cleanString(divider)).append("\"").append(",")
				.append("\"").append(contentIndex).append("\"").append(",")
				.append("\"").append(cleanString(thisBlock)).append("\"")
				.append("\r\n");
				//System.out.println(row);
				pw.print(row+"");
				//pw.print(separator);
				pw.flush();
			}
		}
	}

	/**
	 * 
	 * @param inputString
	 * @return
	 */
	private String cleanString(String inputString) {
		inputString = inputString.replace("\n", " ");
		inputString = inputString.replace("\r", " ");
		inputString = inputString.replace("\\", "\\\\");
		inputString = inputString.replace("\"", "'");

		return inputString;
	}

	/**
	 * 
	 * @param content
	 * @return
	 */
	private List<String> breakSentenceToBlocks(String content) {
		List<String> blocks = new ArrayList<>();

		//if there is only 30 chars, return empty
		if(content.length() < minContentLength) {
			return blocks;
		}
		//if its less than the contentlength, add it to the block and return
		if(content.length() <= contentLength) {
			blocks.add(content);
		}
		else
		{
			Reader reader = new StringReader(content);
			DocumentPreprocessor dp = new DocumentPreprocessor(reader);
			List<String> sentenceList = new ArrayList<String>();

			for (List<HasWord> sentence : dp) {
				// SentenceUtils not Sentence
				String sentenceString = SentenceUtils.listToString(sentence);
				sentenceList.add(sentenceString);
			}

			blocks = createChunks(sentenceList, blocks);
			
			//			blocks = recursivelyBreakParagraphs(sentenceList.remove(0), new StringBuilder(), sentenceList, blocks);
			//			blocks = recursivelyBreakParagraphs2(sentenceList, blocks);
		}
		return blocks;
	}

	public List<String> createChunks(List<String> sentences, List<String> chunks) {
		StringBuilder currentChunk = new StringBuilder();

		for (String sentence : sentences) {
			if (currentChunk.length() + sentence.length() <= contentLength) {
				if (currentChunk.length() > 0) {
					currentChunk.append(" "); // Add space for sentence separation
				}
				currentChunk.append(sentence);
			} else {
				String chunk = currentChunk.toString();
				int overlapIndex = getOverlapIndex(chunk, overlapLength);
				String overlap = chunk.substring(overlapIndex);
				chunks.add(chunk);
				currentChunk = new StringBuilder(overlap).append(" ").append(sentence);
			}
		}

		if (currentChunk.length() > 0) {
			chunks.add(currentChunk.toString());
		}

		return chunks;
	}
	
//	public List<String> chunkString(String stringToChunk, List<String> chunks) {
//		StringBuilder currentChunk = new StringBuilder();
//
//		int stringLength = stringToChunk.length();
//		int counter = 0;
//		while(counter < stringLength) {
//			// more content to process
//			String newPortionToChunk = null;
//			if(stringLength > counter+contentLength) {
//				newPortionToChunk = stringToChunk.substring(counter, counter+contentLength);
//			} else {
//				newPortionToChunk = stringToChunk.substring(counter);
//			}
//			counter += contentLength;
//
//			// add to the current chunk
//			currentChunk.append(newPortionToChunk);
//			// store that
//			chunks.add(currentChunk.toString());
//			
//			// if we want overlap
//			// set the new currentChunk to be a portion of the current chunk we added
//			if(overlapLength > 0) {
//				String chunk = currentChunk.toString();
//				int overlapIndex = getOverlapIndex(chunk, overlapLength);
//				String overlap = chunk.substring(overlapIndex);
//				chunks.add(chunk);
//				currentChunk = new StringBuilder(overlap).append(" ");
//			}
//		}
//
//		if (currentChunk.length() > 0) {
//			chunks.add(currentChunk.toString());
//		}
//
//		System.out.println("chunkString >>> " + chunks.size());
//		return chunks;
//	}

	////////////////////////////////////////

	/*
	 * Testing
	 */

	/**
	 * 
	 * @param chunk
	 * @param overlapLength
	 * @return
	 */
	private int getOverlapIndex(String chunk, int overlapLength) {
		int endIndex = chunk.length() - 1;
		int overlapIndex = Math.max(0, endIndex - overlapLength);
		int lastSentenceIndex = chunk.lastIndexOf('.', overlapIndex);
		return lastSentenceIndex >= 0 ? lastSentenceIndex + 1 : overlapIndex;
	}


	private List<String> recursivelyBreakParagraphs(String curSentence, StringBuilder curBlock, List <String> sentenceList, List <String> blockList)
	{
		int lengthRemaining = contentLength - curBlock.length();
		// if not within 10% of limit.. break the sentence
		// and get to words that get close to the sentence
		//System.err.println(lengthRemaining + " <<>>" + curSentence.length());
		System.err.println("Sentence " + curSentence);
		if(curSentence.length() <= lengthRemaining)
		{
			curBlock.append(" ").append(curSentence);
		}

		else if(contentLength - curBlock.length() < tokenLimit*contentLength)
		{
			StringBuilder newSentence = getWordsForLength(curSentence, lengthRemaining);
			curBlock.append(" ").append(newSentence);
			curSentence = curSentence.replace(newSentence + "", "");
			//System.err.println(" curBlock is set to " + curBlock.length());
			blockList.add(curBlock +"");
			curBlock = new StringBuilder(curSentence);
		}
		if(sentenceList.size() > 0)
		{
			curSentence = sentenceList.remove(0);
		}
		else
		{
			blockList.add(curBlock+"");
			return blockList;
		}
		return 	recursivelyBreakParagraphs(curSentence, curBlock, sentenceList, blockList);
	}

	private List<String> recursivelyBreakParagraphs2(List <String> sentenceList, List <String> blockList)
	{

		StringBuilder curBlock = new StringBuilder();
		for(int sentIndex = 0;sentenceList.size() >0;sentIndex++)
		{
			String curSentence = sentenceList.remove(0);
			int lengthRemaining = contentLength - curBlock.length();
			// if not within 10% of limit.. break the sentence
			// and get to words that get close to the sentence
			//System.err.println(lengthRemaining + " <<>>" + curSentence.length());
			//System.err.println("Sentence " + curSentence);
			if(curSentence.length() <= lengthRemaining)
			{
				curBlock.append(" ").append(curSentence);
			}
			else if(contentLength - curBlock.length() < tokenLimit*contentLength)
			{
				StringBuilder newSentence = getWordsForLength(curSentence, lengthRemaining);
				curBlock.append(" ").append(newSentence);
				curSentence = curSentence.replace(newSentence + "", "");
				//System.err.println(" curBlock is set to " + curBlock.length());
				blockList.add(curBlock +"");
				curBlock = new StringBuilder(curSentence);
			}
		}
		blockList.add(curBlock+"");
		return blockList;
	}

	/**
	 * 
	 * @param sentence
	 * @param length
	 * @return
	 */
	private StringBuilder getWordsForLength(String sentence, int length) {
		// need to use spacy to get words
		StringBuilder retSentence = new StringBuilder();
		String[] words = sentence.split("\\s+");
		for (int i = 0; i < words.length; i++)  {
			// You may want to check for a non-word character before blindly
			// performing a replacement
			// It may also be necessary to adjust the character class
			words[i] = words[i].replaceAll("[^\\w]", "");
			if((retSentence + words[i]).length() < length) {
				retSentence.append(words[i]).append(" ");
			}
			else {
				break;
			}
		}
		return retSentence;
	}
	
	public void close() {
		try {
			this.fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.pw.close();
	}


	public static void main(String [] args)
	{
		CSVWriter writer = new CSVWriter("C:/temp/csv.out");
		String content = "";

//		content = "I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +
//				"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." +"I bought mine back in 2018 & probably only use it once a month, maybe twice. It's now 2013 & it's still working like a dream. I mostly use it when I make tacos, cutting up onions & tomatoes, but recently I've started using it to cut french fries. The smallest blade makes perfect McDonalds sized fries (well almost, you have to cut the potato in half so they're short, but they're nice & thin like McDonalds). The larger blade cuts the perfect thick steak-cut fries. I'm here today leaving a review 5 years later because I was telling a friend how great it was & thought I should finally review this product. Buy buy buy!!!" +
//				"This is by far my most favorite kitchen gadget I've ever purchased. It cuts my prep time down immensely WHILE making my food cuts consistent and pretty! I love the different sizes/cut options. I use it for breakfast, lunch, dinner, meal prepping at least 3-5 times a week. Super easy to use, easy to clean and easy to store. Be careful because it is sharp, but designed in a way you should not have to touch the blades. If food gets stuck in the cut area, I use my dish brush to get it out when I am done prepping"+
//				"I love cooking from scratch, but chopping and dicing have always been the part I couldn't stand. This with all its little add ons has worked amazing cutting my prep time down tremendously! Within minutes I can dice up tomatoes, onions, and hard boiled eggs. Grip on the bottom secures it so it doesn't go flying. Clean up is super simple and I am so grateful for the little bristle brush they send with it- makes cleaning a heck of a lot easier! Not cheaply made- great quality!" +
//				"I do a lot of fine chopping for vegetable type salads. I saw this and my first inclination was; here we go another gimmicky Gadget. I read the reviews, I ordered it anyway, desperate for some solution to all this chopping.\r\n" + 
//				"\r\n" + 
//				"I received it today. It was well packaged, and yet in the back of my mind I'm thinking oh dear I've fallen for a gimmicky Gadget. I promptly put it to work cutting up 10 bell peppers, two onions, and an entire can of large round black olives. This thing worked perfectly! I might go out on a limb and say it seems almost magical. I was done in no time. It was easy to clean up. It came with little gimmicky looking cleaning tools for the upper pusher. And yet they were not gimmicky at all, they were amazingly engineered, perfect little cleaning tools. Now, if you are the type to leave things in the sink for hours after using them I can imagine this thing would be hard to clean. I cleaned it immediately and it was so easy to do. The pieces are cut so beautifully. It doesn't smash, it doesn't crush, it cuts lovely pieces! This was easier to use than my food processor.\r\n" + 
//				"\r\n" + 
//				"If you are on the fence, jump off of that fence and buy this thing. You will not regret it." +
//				"I hate crying during my battles with onions and chopping tomatoes and getting nasty guts, seeds, and moisture on my hands. Also, I got tired of accidentally cutting myself with a knife because I think I'm some chef working at a Michelin-star restaurant. No! Those days are long over. I got real with myself and invested in one of these and I will never go back. I'll leave the fancy knife skills to the pros. The most I do is cut my onion in half to put on the chopper and press down. No tears, no wet hands, no cuts! I have perfectly chopped veggies in seconds. And the pieces are uniform too! Do yourself a favor and get one of these. You won't regret it!" +
//				"I�m 75 and been cooking all my life - it�s actually therapeutic for me and it�s my favorite hobby. Always proud of my knife skills but wanted to see what the hype about this was. I was pleasantly surprised at how easy this was to use and the vegetables were cut evenly with no problems. Easy to clean and love the extras that came with it. I use it a lot more than I thought I would. I would highly recommend." ;

		writer.breakSentenceToBlocks(content);
	}


}
