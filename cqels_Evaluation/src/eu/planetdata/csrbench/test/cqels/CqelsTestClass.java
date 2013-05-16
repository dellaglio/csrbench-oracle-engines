package eu.planetdata.csrbench.test.cqels;

import eu.planetdata.csrbench.test.gsonUtilities.EngineResults;
import eu.planetdata.csrbench.test.gsonUtilities.ExternalBinding;
import eu.planetdata.csrbench.test.gsonUtilities.ExternalRelation;
import eu.planetdata.csrbench.test.gsonUtilities.Head;
import eu.planetdata.csrbench.test.gsonUtilities.InternalBinding;
import eu.planetdata.csrbench.test.gsonUtilities.InternalBindingSerializer;
import eu.planetdata.csrbench.test.gsonUtilities.InternalRelation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;

public class CqelsTestClass {

	private static int cont = 0;
	private static long initialts = 0;
	private static long tsDiff = 0;

	private static ExecContext context;

	private static ArrayList<BindingSet> bindList;
	private static RepositoryConnection conn;

	private static String jsonOutputDir;

	private static String[] testSet;

	private final static Logger logger = LoggerFactory.getLogger(CqelsTestClass.class);

	public static void main(String[] args){
		try {

			testSet = Config.getInstance().getTestSet();
			jsonOutputDir = Config.getInstance().getJsonOutputDir();
			
			if(!jsonOutputDir.endsWith("/"))
				jsonOutputDir = jsonOutputDir + "/";

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
				File f = new File("CQELS_Context");
				if(!f.exists())
					f.mkdir();
				context = new ExecContext(f.getAbsolutePath(), false);
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

	public static void doTest(final int testIdentifier) throws InterruptedException, RepositoryException, MalformedQueryException, QueryEvaluationException{

		final int CONTROL_TEST = 0;
		final int TEST_QUERY_1 = 1;
		final int TEST_QUERY_2 = 2;
		final int TEST_QUERY_3 = 3;
		final int TEST_QUERY_4 = 4;
		final int TEST_QUERY_5 = 5;
		final int TEST_QUERY_6 = 6;
		final int TEST_QUERY_7 = 7;

		String query = null;
		OracleTestGenerator oracleStream = null;

		switch (testIdentifier) {

		case CONTROL_TEST:

			query = "SELECT ?s ?p ?o " +
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 3s SLIDE 3s] { ?s ?p ?o } " +
					"}";
			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		case TEST_QUERY_1:


			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " +
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" + 
					"SELECT ?sensor ?obs " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 10s SLIDE 10s] { " +
					"?obs om-owl:observedProperty weather:_AirTemperature ; " +
					"om-owl:procedure ?sensor ; " +  
					"om-owl:result ?res . " +  
					"?res om-owl:floatValue ?value . " + 
					"} " +
					"FILTER(?value > 80) " +
					"}";

			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		case TEST_QUERY_2:

			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
					"SELECT ?sensor ?obs " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 1s SLIDE 1s] { " +
					"?obs om-owl:observedProperty weather:_AirTemperature ; " +
					"om-owl:procedure ?sensor ; " +  
					"om-owl:result ?res . " +  
					"?res om-owl:floatValue ?value . " + 
					"} " +
					"FILTER(?value > 80) " +
					"}";
			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		case TEST_QUERY_3:

			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
					"SELECT ?sensor ?obs ?value " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 4s SLIDE 4s] { " +
					"?obs om-owl:observedProperty weather:_RelativeHumidity ; " + 
					"om-owl:procedure ?sensor ; " +
					"om-owl:result ?res . " + 
					"?res om-owl:floatValue ?value . " + 
					"} " +
					"FILTER(?value < 49) " +  
					"FILTER(?value > 24) " +
					"}";
			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		case TEST_QUERY_4:

			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
					"SELECT (AVG(?value) AS ?avg) " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 4s SLIDE 4s] { " +
					"?obs om-owl:observedProperty weather:_AirTemperature ; " + 
					"om-owl:procedure ?sensor ; " +
					"om-owl:result ?res . " + 
					"?res om-owl:floatValue ?value . " + 
					"} " +
					"FILTER(?value > 80) " +
					"}";
			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;
		case TEST_QUERY_5:

			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
					"SELECT ?sensor ?obs " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 5s SLIDE 1s] { " +
					"?obs om-owl:observedProperty weather:_AirTemperature ; " + 
					"om-owl:procedure ?sensor ; " +
					"om-owl:result ?res . " + 
					"?res om-owl:floatValue ?value . " + 
					"} " +
					"FILTER(?value > 80) " +
					"}";
			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		case TEST_QUERY_6:

			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " + 
					"SELECT ?sensor ?ob1 ?value1 ?obs " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 5s SLIDE 5s] { " +
					"?ob1 om-owl:procedure ?sensor ; " + 
					"om-owl:observedProperty weather:_AirTemperature ; " +           
					"om-owl:result [om-owl:floatValue ?value1] . " + 
					"?obs om-owl:procedure ?sensor ; " + 
					"om-owl:observedProperty weather:_AirTemperature ; " +        
					"om-owl:result [om-owl:floatValue ?value] . " + 
					"} " + 
					"FILTER(?value1 > ?value) " + 
					"FILTER(?value > 75) " + 
					"}";
			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		case TEST_QUERY_7:

			query = "PREFIX om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> " + 
					"PREFIX weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> " +
					"PREFIX sens-obs: <http://knoesis.wright.edu/ssw/> " + 
					"SELECT ?sensor ?ob1 " + 
					"WHERE { " +
					"STREAM <http://ex.org/streams/test> [RANGE 5s SLIDE 5s] { " +
					"?ob  om-owl:procedure sens-obs:System_C1190 ; " + 
					"om-owl:observedProperty weather:_AirTemperature ; " +           
					"om-owl:result [om-owl:floatValue ?value] . " + 
					"?ob1 om-owl:procedure ?sensor ; " + 
					"om-owl:observedProperty weather:_AirTemperature ; " +        
					"om-owl:result [om-owl:floatValue ?value1] . " +
					"} " + 
					"FILTER(?value1 > ?value) " + 
					"}";

			oracleStream = new OracleTestGenerator(context, "http://ex.org/streams/test", 1, conn, bindList);

			break;

		default:
			System.exit(0);
			break;
		}

		Thread t = new Thread(oracleStream);

		ContinuousSelect selQuery=context.registerSelect(query);

		t.start();

		//		new Thread(new JsonWriter(sleep, testIdentifier)).start();

		initialts = System.currentTimeMillis();

		selQuery.register(new ContinuousListener()
		{
			
			InternalBinding intBinding;
			HashMap<String, InternalBinding> intBindingMap = new HashMap<String, InternalBinding>();

			ExternalBinding extBinding;
			ArrayList<ExternalBinding> extBindingList = new ArrayList<ExternalBinding>();

			ArrayList<String> varName;

			EngineResults engineResults = new EngineResults();

			InternalRelation internalRelation;

			Head head;

			ExternalRelation externalRelation;
			ArrayList<ExternalRelation> extRelationList = new ArrayList<ExternalRelation>();	

			Gson gson = new GsonBuilder()
			.registerTypeAdapter(InternalBinding.class, new InternalBindingSerializer())
			.create();
			
			public void update(Mapping mapping){

				BufferedWriter out;
				varName = new ArrayList<String>();
				cont = 0;

				tsDiff = System.currentTimeMillis() - initialts;

				Node partialResult;
				for(Iterator<Var> vars=mapping.vars();vars.hasNext();){

					//Use context.engine().decode(...) to decode the encoded value to RDF Node
					Var var = vars.next();
					varName.add(var.toString().replace("?", ""));
					partialResult = context.engine().decode(mapping.get(var));
					//					System.out.println(partialResult.toString() + " , " + System.currentTimeMillis());

					intBinding = new InternalBinding();

					intBinding = new InternalBinding();

					if(partialResult.toString().contains("^^")){
						intBinding.setValue(partialResult.toString().substring(0, partialResult.toString().indexOf("^")).replace("\"", ""));
						intBinding.setType("literal");
						intBinding.setDatatype(partialResult.toString().substring(partialResult.toString().indexOf("^") + 2, partialResult.toString().length()));
					} else {
						intBinding.setValue(partialResult.toString());
						intBinding.setType("uri");
						intBinding.setDatatype("");
					}

					intBindingMap.put((String) varName.toArray()[cont], intBinding);

					cont++;

				}

				extBinding = new ExternalBinding();
				extBinding.setBinding(intBindingMap);
				intBindingMap = new HashMap<String, InternalBinding>();
				extBinding.setTimestamp(tsDiff);

				extBindingList.add(extBinding);

				internalRelation = new InternalRelation();
				internalRelation.addElements(extBindingList);

				extBindingList = new ArrayList<ExternalBinding>();
				head = new Head();
				head.addElements(varName);

				externalRelation = new ExternalRelation();
				externalRelation.setTimestamp(tsDiff);
				externalRelation.setHead(head);
				externalRelation.setResults(internalRelation);
				extRelationList.add(externalRelation);

				engineResults.addElements(extRelationList);

				try {
					out = new BufferedWriter(new FileWriter(jsonOutputDir + "cqels-answer-" + testIdentifier + ".json"));
					out.write(gson.toJson(engineResults));
					logger.info(gson.toJson(engineResults));
					out.close();
				} 
				catch (IOException e) 
				{ 
					e.printStackTrace();
				}
				//
			} 
		});

	}

}
