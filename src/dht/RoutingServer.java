package dht;

import java.util.Hashtable;

public class RoutingServer {
	Hashtable<String,String> data = null;
	int isMaster;
	int lastRouting;
	String nextServer;
	String prevServr;
	public RoutingServer(boolean master)
	{
		isMaster=master;
		data= new Hashtable<String, String>();
	}
	public String hashString(String value)
	{
		return value;
	}
	
	public boolean insert(String key, String value)
	{
		
	}
}
