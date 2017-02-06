package core;


import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalINIConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;

public class Config {

	private HierarchicalINIConfiguration iniConfObj;
	
	public Config(String configFile) throws ConfigurationException
	{
	    iniConfObj = new HierarchicalINIConfiguration(configFile);
	    iniConfObj.setAutoSave(true);
	}

	public SubnodeConfiguration getConfig()
	{
		SubnodeConfiguration sObj = iniConfObj.getSection("general");
		return sObj;
	}

	
}