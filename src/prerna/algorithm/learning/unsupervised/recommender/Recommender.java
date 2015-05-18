package prerna.algorithm.learning.unsupervised.recommender;

import org.apache.commons.math3.linear.BlockRealMatrix;

/**
 * Duties of Recommender
 * 	handling the update of top users with new information
 * 	figure out when to stop running in the event of streaming
 * 	getting and receiving new top users
 */
public class Recommender {
	
	private SimilarityAnalytics algorithm;
	private RecommendedUsers topUsers;
	private final String user;
	
    public Recommender(String u) {
    	user = u;
    	algorithm = new VectorSimilarity();
    	topUsers = new RecommendedUsers();
    }
	public Recommender(String u, SimilarityAnalytics a) {
        user = u;
    	algorithm = a;
        topUsers = new RecommendedUsers();
    }
    
	//Set which algorithm to use to calculate the Similarity Scores to the user
    public void setMode(SimilarityAnalytics a) {
        algorithm = a;
        topUsers = new RecommendedUsers();
    }
    
    //update the top users with new information
    public void update(BlockRealMatrix data, String[] ids) {
		double[] simScores = algorithm.runAlgorithm(data);
		for(int i=0; i<ids.length; i++) {
			topUsers.insert(ids[i], simScores[i]);//turn this into a stream and/or filter out really bad scores
		}
    }
    
    public void update(double[][] data, String[] ids){
    	BlockRealMatrix d = new BlockRealMatrix(data);
    	update(d, ids);
    }
    
    public String[] getAllRecommendedUsers() {
    	return topUsers.getAllUsers();
    }
    
    public String[] getRecommendedUsers(int num){
    	return topUsers.getTop(num);
    }
    
    //Get the ID of the user this recommender is tied to
    public String getUser(){
    	return user;
    }
}
