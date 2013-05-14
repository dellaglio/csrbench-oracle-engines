package eu.planetdata.csrbench.test.cqels;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Config {
	private static Config _instance = null;
	private static final Logger logger = LoggerFactory.getLogger(Config.class); 
	
	private Configuration config;
	
	private Config(){
		try {
			config = new PropertiesConfiguration("setup.properties");
		} catch (ConfigurationException e) {
			logger.error("Error while reading the configuration file", e);
		}
	}
	
	public File getRepoDir(){
		String dir = config.getString("cqelstest.repo.datadir");
		File ret = new File(dir);
		return ret;
	}
	
	public String[] getTestSet(){
		return config.getStringArray("testset");
	}
	
	public String getJsonOutputDir(){
		String dir = config.getString("cqelstest.jsonfile.outputdir");
		File f = new File(dir);
		
		if(!f.exists())
			f.mkdir();
		
		return dir;
	}
		
	public static Config getInstance(){
		if(_instance==null)
			_instance=new Config();
		return _instance;
	}
	
}
