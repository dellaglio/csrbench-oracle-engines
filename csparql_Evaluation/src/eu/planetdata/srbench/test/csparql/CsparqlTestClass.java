package eu.planetdata.srbench.test.csparql;
import java.text.ParseException;
import java.util.ArrayList;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.engine.CsparqlEngine;
import eu.larkc.csparql.engine.CsparqlEngineImpl;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;

public class CsparqlTestClass {

	private static CsparqlEngine engine;

	private static ArrayList<BindingSet> bindList;
	private static RepositoryConnection conn;

	private static String jsonOutputDir;

	private static String[] testSet;

	private final static Logger logger = LoggerFactory.getLogger(CsparqlTestClass.class);

	public static void main(String[] args){

		try {

			testSet = Config.getInstance().getTestSet();
			jsonOutputDir = Config.getInstance().getJsonOutputDir();

			bindList = new ArrayList<BindingSet>();

			String baseURI = "http://www.streamreasoning.org/schema/benchmark#";

			URI graphList = new URIImpl(baseURI+"graphsList");
			URI hasTimestamp = new URIImpl(baseURI+"hasTimestamp");

			Repository repo =  new SailRepository(new NativeStore(Config.getInstance().getRepoDir(), "cspo,cops"));

			try {
				repo.initialize();
			} catch (RepositoryException e) {
				e.printStackTrace();
			}

			conn=repo.getConnection();

			String qg="SELECT ?g " +
					"FROM <" + graphList + "> " +
					"WHERE{" +
					"?g <" + hasTimestamp + "> ?timestamp. " +
					"}";
			TupleQuery q=conn.prepareTupleQuery(QueryLanguage.SPARQL, qg);
			TupleQueryResult tqr=q.evaluate();		
			while (tqr.hasNext()){
				bindList.add(tqr.next());
			}

			for(final String testIDString : testSet){
				logger.info("Starting test number {}",testIDString);
				doTest(Integer.parseInt(testIDString));
				Thread.sleep(2000);
			}

		} catch (RepositoryException e) {
			logger.error("Error while initializing the repository", e);
		} catch (MalformedQueryException e) {
			logger.error("Error while querying the repository", e);
		} catch (QueryEvaluationException e) {
			logger.error("Error while evaluatiting query on the repository", e);
		} catch (InterruptedException e) {
			logger.error("Thread Sleep Error", e);
		}
	}

	private static void doTest(int testIdentifier) {

		try {

			long sleep = 1000;

			CsparqlQueryResultProxy c1;
			String query = null;
			RdfStream tg = null;
			long stepValue = 0;

			String streamURI = "http://ex.org";

			//Engine Test Constants

			final int CONTROL_TEST = 0;
			final int TEST_QUERY_1 = 1;
			final int TEST_QUERY_2 = 2;
			final int TEST_QUERY_3 = 3;
			final int TEST_QUERY_4 = 4;
			final int TEST_QUERY_5 = 5;
			final int TEST_QUERY_6 = 6;
			final int TEST_QUERY_7 = 7;


			switch (testIdentifier) {

			case CONTROL_TEST:

				query = "REGISTER QUERY test AS " +
						"SELECT ?s ?p ?o " +
						"FROM STREAM <http://ex.org/streams/test> [RANGE 2s STEP 2s] " +
						"WHERE { " +
						"?s ?p ?o . " +
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 2000;

				break;

			case TEST_QUERY_1:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
						"SELECT ?sensor ?obs " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 10s STEP 10s] " +
						"WHERE { " +
						"?obs om-owl:observedProperty weather:_AirTemperature ; " +
						"om-owl:procedure ?sensor ; " +  
						"om-owl:result ?res . " +  
						"?res om-owl:floatValue ?value . " + 
						"FILTER(?value > 80) " +
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 10000;

				break;

			case TEST_QUERY_2:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
						"SELECT ?sensor ?obs " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 1s STEP 1s] " +
						"WHERE { " +
						"?obs om-owl:observedProperty weather:_AirTemperature ; " +
						"om-owl:procedure ?sensor ; " +  
						"om-owl:result ?res . " +  
						"?res om-owl:floatValue ?value . " + 
						"FILTER(?value > 80) " +
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 1000;

				break;

			case TEST_QUERY_3:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
						"SELECT ?sensor ?obs ?value " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 4s STEP 4s] " +
						"WHERE { " +
						"?obs om-owl:observedProperty weather:_RelativeHumidity ; " + 
						"om-owl:procedure ?sensor ; " +
						"om-owl:result ?res . " + 
						"?res om-owl:floatValue ?value . " + 
						"FILTER(?value < 49) " +  
						"FILTER(?value > 24) " +
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 4000;

				break;

			case TEST_QUERY_4:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
						"SELECT (AVG(?value) AS ?avg) " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 4s STEP 4s] " +
						"WHERE { " +
						"?obs om-owl:observedProperty weather:_AirTemperature ; " + 
						"om-owl:procedure ?sensor ; " +
						"om-owl:result ?res . " + 
						"?res om-owl:floatValue ?value . " + 
						"FILTER(?value > 80) " +  
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 4000;

				break;

			case TEST_QUERY_5:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
						"SELECT ?sensor ?obs " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 5s STEP 1s] " +
						"WHERE { " +
						"?obs om-owl:observedProperty weather:_AirTemperature ; " + 
						"om-owl:procedure ?sensor ; " +
						"om-owl:result ?res . " + 
						"?res om-owl:floatValue ?value . " + 
						"FILTER(?value > 80) " +  
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 1000;

				break;

			case TEST_QUERY_6:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
						"SELECT ?sensor ?ob1 ?value1 ?obs " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 5s STEP 5s] " +
						"WHERE { " +
						"?ob1 om-owl:procedure ?sensor ; " + 
						"om-owl:observedProperty weather:_AirTemperature ; " +           
						"om-owl:result [om-owl:floatValue ?value1] . " + 
						"?obs om-owl:procedure ?sensor ; " + 
						"om-owl:observedProperty weather:_AirTemperature ; " +        
						"om-owl:result [om-owl:floatValue ?value]. " + 
						"FILTER(?value1 > ?value) " + 
						"FILTER(?value > 75) " + 
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 5000;

				break;

			case TEST_QUERY_7:

				query = "REGISTER QUERY test AS " +
						"PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
						"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " +
						"PREFIX sens-obs: <http://knoesis.wright.edu/ssw/> " + 
						"SELECT ?sensor ?ob1 " + 
						"FROM STREAM <http://ex.org/streams/test> [RANGE 5s STEP 5s] " +
						"WHERE { " +
						"?ob  om-owl:procedure sens-obs:System_C1190 ; " + 
						"om-owl:observedProperty weather:_AirTemperature ; " +           
						"om-owl:result [om-owl:floatValue ?value] . " + 
						"?ob1 om-owl:procedure ?sensor ; " + 
						"om-owl:observedProperty weather:_AirTemperature ; " +        
						"om-owl:result [om-owl:floatValue ?value1]. " + 
						"FILTER(?value1 > ?value) " + 
						"}";
				tg = new OracleTestGenerator("http://ex.org/streams/test", 1, conn,bindList, sleep);

				stepValue = 5000;

				break;

			default:
				System.exit(0);
				break;
			}

			// Initialize C-SPARQL Engine 
			engine = new CsparqlEngineImpl();
			engine.initialize();

			// Register an RDF Stream

			//Start Streaming

			final Thread t = new Thread((Runnable) tg);
			engine.registerStream(tg);

			// Register a C-SPARQL query

			c1 = null;

			try {
				c1 = engine.registerQuery(query);
				//			System.out.println("Query Registration Time: " + System.currentTimeMillis());
				tg.put(new RdfQuadruple(streamURI + "/s", streamURI + "/p", streamURI + "/o", System.currentTimeMillis()));

				t.start();


			} catch (final ParseException ex) {
				System.out.println("errore di parsing: " + ex.getMessage());
			}

			// Attach a Result Formatter to the query result proxy 

			if (c1 != null) {
				//				c1.addObserver(new ConsoleFormatter());
				c1.addObserver(new JsonFormatter(stepValue, testIdentifier, jsonOutputDir));
			}

			Thread.sleep(60000);


			engine.unregisterQuery(c1.getId());
			engine.unregisterStream(tg.getIRI());
			engine.destroy();
			engine = null;

		} catch (InterruptedException e) {
			logger.error("Thread Sleep Error", e);
		}

	}

}
