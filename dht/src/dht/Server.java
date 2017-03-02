package dht;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

public class Server {
	SortedMap<String,String> data = null;
	boolean isMaster;
	long leastHash;
	long maxHash;
	public Server(boolean master,long least,long max)
	{
		isMaster=master;
		leastHash=least;
		maxHash=max;
	}
	public String hashString(String value)
	{
		return value;//not used yet
	}
	
	public String changeRanges(long low, long high,String newData)
	{
		leastHash=low;
		maxHash=high;
		String result="";
		String current_key,current_value;
		Iterator<String> iter=data.tailMap(Long.valueOf(maxHash).toString()).keySet().iterator();
		if(iter.hasNext())
		{
			current_key=iter.next();
			current_value=data.get(current_key);
			result=result+current_key+","+current_value;
			data.remove(current_key,current_value);
		}
		while(iter.hasNext())
		{
			current_key=iter.next();
			current_value=data.get(current_key);
			result=result+"_"+current_key+","+current_value;
			data.remove(current_key,current_value);
		}
		return result;
	}
	
	public String sendData()
	{
		String result="";
		Iterator<String> iter=data.keySet().iterator();
		String current_key;
		String current_value;
		if(iter.hasNext())
		{
			current_key=iter.next();
			current_value=data.get(current_key);
			result=result+current_key+","+current_value;
		}
		while(iter.hasNext())
		{
			current_key=iter.next();
			current_value=data.get(current_key);
			result=result+"_"+current_key+","+current_value;
		}
		data.clear();
		return result;
	}
	
	public String action(String execute)
	{
		String[] split=execute.split(",");
		String result;
		if(split[1].equals("insert")) result="Answer-"+insert(split[0],split[2]);
		else if (split[1].equals("query")) result="Answer-"+query(split[0]);
		else if (split[1].equals("delete")) result="Answer-"+delete(split[0]);
		else if (split[0].equals("NewRange")) result="OK-"+changeRanges(Long.parseLong(split[1]),Long.parseLong(split[2]),split[3]);
		else if (split[0].equals("Leaving")) result="Data-"+sendData();
		else result="Error";
		return result;
	}
	
	public String insert(String key, String value)
	{
		data.put(key,value);
		return "Done";
	}
	
	public String delete (String key)
	{
		if(data.containsKey(key))
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
			if(data.containsKey(key))
			{
				String current_value=data.get(key);
				result=key+","+current_value;
				return result;
			}
			return null;
		}
	}
	
}
