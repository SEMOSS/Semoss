package prerna.cluster.util.clients;

public interface ICloudClientBuilder {

	/**
	 * Build the client
	 * @return
	 */
	CloudClient buildClient();
	
	/**
	 * 
	 * @return
	 */
	ICloudClientBuilder pullValuesFromSystem();
	
	/**
	 * 
	 * @param rclonePath
	 * @return
	 */
	ICloudClientBuilder setRClonePath(String rclonePath);

	/**
	 * 
	 * @param rcloneConfigF
	 * @return
	 */
	ICloudClientBuilder setRCloneConfigFolder(String rcloneConfigF);

	/**
	 * 
	 * @return
	 */
	String getRClonePath();
	
	/**
	 * 
	 * @return
	 */
	String getRCloneConfigFolder();
	
}
