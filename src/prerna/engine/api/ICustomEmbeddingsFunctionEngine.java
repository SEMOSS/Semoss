package prerna.engine.api;

import java.io.File;
import java.util.Map;

/**
 * Interface for specialized function engines that generate custom embeddings from documents.
 * 
 * <p>This interface extends {@link IFunctionEngine} to provide specialized functionality
 * for processing documents and generating embeddings that can be stored in vector databases.
 * Custom embedding engines typically combine document processing capabilities (such as OCR,
 * text extraction, or content analysis) with embedding generation to create vector
 * representations suitable for semantic search and similarity matching.</p>
 * 
 * <p>These engines are particularly useful for:</p>
 * <ul>
 *   <li><strong>Document Analysis:</strong> Processing various document formats (PDF, images, etc.)</li>
 *   <li><strong>Content Extraction:</strong> Extracting text content using OCR or parsing</li>
 *   <li><strong>Embedding Generation:</strong> Creating vector embeddings from extracted content</li>
 *   <li><strong>Vector Database Integration:</strong> Preparing embeddings for storage in vector databases</li>
 * </ul>
 * 
 * <p>Implementations typically output embedding data in CSV format that can be directly
 * consumed by vector database engines for indexing and search operations.</p>
 * 
 * @see {@link IFunctionEngine} for base function engine capabilities
 * @see {@link IVectorDatabaseEngine} for vector database operations
 * @see {@link FunctionTypeEnum} for available custom embedding engine types
 * @author SEMOSS
 */
public interface ICustomEmbeddingsFunctionEngine extends IFunctionEngine {

	/**
	 * Determines whether this engine can process the specified document file.
	 * 
	 * <p>This method checks if the provided file is of a type and format that
	 * this custom embeddings engine can handle. Different engines may support
	 * different file types (e.g., PDF, images, text files) based on their
	 * underlying processing capabilities.</p>
	 * 
	 * @param fileToProcess The file to check for processing compatibility
	 * @return true if this engine can process the file, false otherwise
	 */
	boolean canProcessDocument(File fileToProcess);
	
	/**
	 * Processes a document and generates embeddings, saving the results to a CSV file.
	 * 
	 * <p>This method performs the complete pipeline of document processing and
	 * embedding generation:</p>
	 * <ol>
	 *   <li>Analyzes and extracts content from the input document</li>
	 *   <li>Processes the extracted content (chunking, cleaning, etc.)</li>
	 *   <li>Generates vector embeddings for the processed content</li>
	 *   <li>Saves the embeddings and metadata to the specified CSV file</li>
	 * </ol>
	 * 
	 * <p>The output CSV format typically includes columns for embeddings, source
	 * metadata, content chunks, and other relevant information needed by vector
	 * database engines.</p>
	 * 
	 * @param outputCsvFilePath The absolute path where the embedding CSV file will be created
	 * @param fileToProcess The document file to process and generate embeddings from
	 * @param parameters Additional processing parameters such as chunking strategies,
	 *                   embedding model settings, or output format options
	 * @return The number of embedding rows written to the output CSV file
	 * @throws RuntimeException If document processing or embedding generation fails
	 */
	int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters);

}