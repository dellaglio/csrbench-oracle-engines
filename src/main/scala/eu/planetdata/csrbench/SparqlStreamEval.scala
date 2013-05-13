package eu.planetdata.csrbench

import es.upm.fi.oeg.morph.esper.EsperServer
import es.upm.fi.oeg.morph.esper.EsperProxy
import eu.planetdata.srbench.feed.LsdDataFeed
import es.upm.fi.oeg.siq.tools.ParameterUtils
import java.io.FileOutputStream
import eu.planetdata.srbench.ResultsReceiver
import org.slf4j.LoggerFactory
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator
import scala.compat.Platform
import java.net.URI


class SparqlStreamEval(propsFile:String) {
  val logger = LoggerFactory.getLogger(this.getClass)  
  val props=ParameterUtils.load(getClass.getResourceAsStream(propsFile))
  val esper=new EsperServer           
  val eval = new QueryEvaluator(props,esper.system)
  esper.startup    
  val proxy=new EsperProxy(esper.system)
  val feed=new LsdDataFeed(props,proxy)
  val mapping=new URI(props.getProperty("srbench.r2rml.mapping"))
  val rate = props.getProperty("feed.rate").toLong
  val query=props.getProperty("srbench.query")
  val serialize=props.getProperty("srbench.serialize")
  val maxtime=props.getProperty("srbench.maxtime").toInt
  
  private def load(q:String)= ParameterUtils.loadQuery("queries/srbench/"+q)
  
  def run{
    val o=new FileOutputStream("results/"+query+".json")    
    val rec=new ResultsReceiver(Platform.currentTime,rate)

    logger.info("pushing")
      eval.listenToQuery(load(query),mapping,rec)
      feed.schedule
      rec.initTime
     Thread.sleep(maxtime)
 
    esper.system.shutdown
    if (serialize.equals("json"))
      rec.jsonize(o)
    else
      rec.serializeAll(System.out)    
    o.close


  }
}

object SparqlStreamEvaluation{
  def main(args:Array[String])={
    val spstream=new SparqlStreamEval("/config/srbench.properties")
    spstream.run
  }
}