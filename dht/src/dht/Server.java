package dht;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

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
	
	public String action(String execute)
	{
		String[] split=execute.split(",");
		String result;
		if(split[1].equals("insert")) result=insert(split[0],split[2]);
		else if (split[1].equals("query")) result=query(split[0]);
		else if (split[1].equals("delete")) result=delete(split[0]);
		else result="Error";
		return "Answer-"+result;
	}
	
	public String insert(String key, String value)
	{
		data.put(key,value);
		return "Done";
	}
	
	public String delete (String key)
	{
		if(data.contains(key))
		{
			String value=data.remove(key);
			return value;
		}
		else return null;
	}
	
	public String query(String key)
	{
		String result="";
		if(key.equals("*")) 
		{
			Set<String> allKeys= data.keySet();
			String current_key;
			String current_value;
			if (allKeys.isEmpty()) return null;
			Iterator<String> iter=allKeys.iterator();
			if(iter.hasNext())
			{
				current_key=(String) iter.next();
				current_value=data.get(current_key);
				result=result+current_key+","+current_value;
			}
			while(iter.hasNext())
			{
				current_key=(String) iter.next();
				current_value=data.get(current_key);
				result=result+"\n"+current_key+","+current_value;
			}
			return result;
		}
		else
		{
			if(data.contains(key))
			{
				String current_value=data.get(key);
				result=key+","+current_value;
				return result;
			}
			return null;
		}
	}
	
}
