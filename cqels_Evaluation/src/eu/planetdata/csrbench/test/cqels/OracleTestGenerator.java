package eu.planetdata.csrbench.test.cqels;

import java.util.ArrayList;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
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

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;

public class OracleTestGenerator extends RDFStream implements Runnable {

	private int key;
	private RepositoryConnection conn;
	private ArrayList<BindingSet> bindList;

	private final static Logger logger = LoggerFactory.getLogger(OracleTestGenerator.class);

	public OracleTestGenerator(ExecContext context, String uri, int key,RepositoryConnection conn, ArrayList<BindingSet> bindList) {
		super(context, uri);
		this.key = key;
		this.bindList = bindList;
		this.conn = conn;
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

		for(final BindingSet bind : bindList){

			long l1 = System.currentTimeMillis();

			try{
				RepositoryResult<Statement> result = conn.getStatements((Resource)null, (URI)null, (Value)null, false, new URIImpl(bind.getValue("g").stringValue()));

				while(result.hasNext()){
					Statement s = result.next();

					if(!s.getObject().toString().contains("http://www.w3.org/2001/XMLSchema#")){
						stream(Node.createURI(s.getSubject().toString()),Node.createURI(s.getPredicate().toString()),Node.createURI(s.getObject().toString()));

					} else{
						stream(Node.createURI(s.getSubject().toString()),Node.createURI(s.getPredicate().toString()),Node.createLiteral(s.getObject().stringValue(), null, XSDDatatype.XSDdouble));
					}
				}

			} catch(RepositoryException e){
				System.out.println("Error while reading the triples in the repository");
			}

			l2 = System.currentTimeMillis() - l1;

			Thread.sleep(1000 - l2);
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}
}