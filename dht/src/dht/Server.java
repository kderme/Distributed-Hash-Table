package dht;

import java.util.Hashtable;

public class Server {
	Hashtable<String,String> data = null;
	boolean isMaster;
	String leastHash=null;
	String maxHash=null;
	public Server(boolean master,String least,String max)
	{
		isMaster=master;
		data= new Hashtable<String, String>();
		leastHash=least;
		maxHash=max;
	}
	public String hashString(String value)
	{
		return value;
	}
	
	public boolean insert(String key, String value)
	{
		data.put(key,value);
		return true;
	}
}
