package prerna.engine.impl.tinker;

import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import prerna.engine.impl.SmssUtilities;
import prerna.util.gson.GsonUtility;

public class JanusEngine extends TinkerEngine {
	private static final Logger LOGGER = LoggerFactory.getLogger(JanusEngine.class);

	@Override
	public void openDB(String propFile) {
		super.openDB(propFile);
		String janusConfFilePath = SmssUtilities.getJanusFile(prop).getAbsolutePath();
		try {
			LOGGER.info("Opening graph: " + janusConfFilePath);
			g = JanusGraphFactory.open(janusConfFilePath);
			LOGGER.info("Done opening graph: " + janusConfFilePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO update path to example
		// semoss app
		String propertiesFile = "C:\\Users\\rramirezjimenez\\Documents\\workspace\\janusgraph-0.2.2-hadoop2\\conf\\janusgraph-berkeleyje-es.properties";
		// test
		// propertiesFile =
		// "C:\\Users\\rramirezjimenez\\Documents\\workspace\\janusgraph-0.2.2-hadoop2\\conf\\janusgraph-berkeleyje.properties";
		Gson gson = GsonUtility.getDefaultGson();

		// graph database set up example
		// GraphOfTheGodsFactory.load((JanusGraph) graph);
		// graph.traversal().V().hasLabel("god").property("type","god").iterate();
		// get labels
		// System.out.println(graph.traversal().V().label().dedup().iterate());
		// graph.traversal().V().hasLabel("god").property("type","god").iterate();
		// graph.traversal().V().hasLabel("monster").property("type","monster").iterate();
		// graph.traversal().V().hasLabel("location").property("type","location").iterate();
		// graph.traversal().V().hasLabel("human").property("type","human").iterate();
		// graph.traversal().V().hasLabel("titan").property("type","titan").iterate();
		//
		//
		// list graph properties
		// ((GraphTraversal<Vertex, Vertex>)
		// graph.traversal()).has("type","god").properties();
		//
		//
		// GraphTraversalSource gt = graph.traversal();
		// List<String> properties = GraphUtility.getAllNodeProperties(gt);
		// HashMap<String, Object> mm = GraphUtility.getMetamodel(gt, "type");
		// System.out.println(gson.toJson(properties));
		// System.out.println(gson.toJson(mm));

		try {
			Graph graph = JanusGraphFactory.open(propertiesFile);
			GraphTraversal<Vertex, Object> gt = graph.traversal().V().has("type", "god").as("god").select("god")
					.dedup();
			while (gt.hasNext()) {
				Object data = gt.next();

				if (data instanceof Map) {
					Map<String, Object> mapData = (Map<String, Object>) data;

				} else {
					// not sure what will happen once we add group bys -> is
					// this a map like above or different???

					// for right now, assuming it is just a single vertex to
					// return
					if (data instanceof Vertex) {
						Vertex vertex = (Vertex) data;
						System.out.println(vertex.value("name") + "");
					} else {
						// some object to return
						Object[] retObject = new Object[] { data };
					}
				}

			}

			gt.close();
			graph.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);

	}
}
