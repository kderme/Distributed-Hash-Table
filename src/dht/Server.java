package dht;

import java.util.Hashtable;

public class Server {
	Hashtable<String,String> data = null;
	int isMaster;
	
	public Server(boolean master)
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
