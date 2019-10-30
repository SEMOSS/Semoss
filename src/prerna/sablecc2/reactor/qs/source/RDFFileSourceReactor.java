package prerna.sablecc2.reactor.qs.source;

import java.io.File;
import java.io.IOException;

import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.engine.impl.rdf.InMemorySesameEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class RDFFileSourceReactor extends AbstractQueryStructReactor {

	private static final String RDF_TYPE = "rdfType";
	private static final String BASE_URI = "baseUri";

	public RDFFileSourceReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FILE_PATH.getKey(), RDF_TYPE, BASE_URI};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();

		String filePath = this.keyValue.get(this.keysToGet[0]);
		filePath = this.insight.getAbsoluteInsightFolderPath(filePath);
		File file = new File(filePath);
		if(!file.exists()) {
			throw new IllegalArgumentException("Unable to location file");
		}
		String rdfFileType = this.keyValue.get(this.keysToGet[1]);
		if(rdfFileType == null || rdfFileType.isEmpty()) {
			rdfFileType = "RDF/XML";
		}
		String baseURI = this.keyValue.get(this.keysToGet[2]);

		// generate the in memory rc 
		RepositoryConnection rc = null;
		try {
			Repository myRepository = new SailRepository(new ForwardChainingRDFSInferencer(new MemoryStore()));
			myRepository.initialize();
			rc = myRepository.getConnection();

			// load in the meta from saved file
			if(rdfFileType.equalsIgnoreCase("RDF/XML")) rc.add(file, baseURI, RDFFormat.RDFXML);
			else if(rdfFileType.equalsIgnoreCase("TURTLE")) rc.add(file, baseURI, RDFFormat.TURTLE);
			else if(rdfFileType.equalsIgnoreCase("BINARY")) rc.add(file, baseURI, RDFFormat.BINARY);
			else if(rdfFileType.equalsIgnoreCase("N3")) rc.add(file, baseURI, RDFFormat.N3);
			else if(rdfFileType.equalsIgnoreCase("NTRIPLES")) rc.add(file, baseURI, RDFFormat.NTRIPLES);
			else if(rdfFileType.equalsIgnoreCase("TRIG")) rc.add(file, baseURI, RDFFormat.TRIG);
			else if(rdfFileType.equalsIgnoreCase("TRIX")) rc.add(file, baseURI, RDFFormat.TRIX);
		} catch(RuntimeException ignored) {
			ignored.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (RDFParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// set the rc in the in-memory engine
		InMemorySesameEngine temportalEngine = new InMemorySesameEngine();
		temportalEngine.setRepositoryConnection(rc);
		temportalEngine.setEngineId("FAKE_ENGINE");
		temportalEngine.setBasic(true);

		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQsType(AbstractQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		qs.setEngine(temportalEngine);
		return qs;
	}

}
