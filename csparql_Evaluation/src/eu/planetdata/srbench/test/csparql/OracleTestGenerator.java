package eu.planetdata.srbench.test.csparql;

import java.util.ArrayList;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

public class OracleTestGenerator extends RdfStream implements Runnable {

	private int key;
	private RepositoryConnection conn;
	private ArrayList<BindingSet> bindList;
	private long sleep;
	private final static Logger logger = LoggerFactory.getLogger(OracleTestGenerator.class);

	public OracleTestGenerator(final String iri, int key, RepositoryConnection conn, ArrayList<BindingSet> bindList, long sleep) {
		super(iri);
		this.key = key;
		this.bindList = bindList;
		this.conn = conn;
		this.sleep = sleep;
	}

	@Override
	public void run() {

		final int TEST_1 = 1;

		switch (key) {

		case TEST_1:
			try {
				test1();
			} catch (RepositoryException e) {
				logger.error("Error while initializing the repository", e);
			} catch (MalformedQueryException e) {
				logger.error("Error while querying the repository", e);
			} catch (QueryEvaluationException e) {
				logger.error("Error while evaluatiting query on the repository", e);
			} catch (InterruptedException e) {
				logger.error("Thread Sleep Error", e);
			}
			break;

		default:
			System.exit(0);
			break;
		}


	}

	private void test1() throws RepositoryException, MalformedQueryException, QueryEvaluationException, InterruptedException{

		long l2 = 0;

		RdfQuadruple qu;
				
		for(final BindingSet bind : bindList){

			long l1 = System.currentTimeMillis();
			try{
				RepositoryResult<Statement> result = conn.getStatements((Resource)null, (URI)null, (Value)null, false, new URIImpl(bind.getValue("g").stringValue()));
				while(result.hasNext()){
					Statement s = result.next();

					if(s.getObject().stringValue().contains("http://"))
						qu = new RdfQuadruple(s.getSubject().stringValue(), s.getPredicate().stringValue(), s.getObject().stringValue(), l1);
					else
						qu = new RdfQuadruple(s.getSubject().stringValue(), s.getPredicate().stringValue(), s.getObject().stringValue() + "^^http://www.w3.org/2001/XMLSchema#integer", l1);
					this.put(qu);
				}
			} catch(RepositoryException e){
				logger.error("Error while interacting with the repository", e);
			}

			l2 = System.currentTimeMillis() - l1;

			Thread.sleep(sleep - l2);
		}
	}
}

