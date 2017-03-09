package dht;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class Server {
	protected SortedMap<String,String> data = null;
	protected boolean isMaster;
	protected String leastHash;
	protected String maxHash;
	protected Console console;
	public Server(boolean master,String least,String max)
	{
		data=new TreeMap<String,String>();
		isMaster=master;
		leastHash=least;
		maxHash=max;
		console= new Console();
	}
	public String hashString(String value)
	{
		return value;//not used yet
	}

	public String changeRanges(String low)
	{
		console.logEntry();
		String result="";
		if(RoutingServer.compareHash(low,leastHash)>=0)
		{
			String current_key,current_value;
			Iterator<String> iter=data.subMap(leastHash,low).keySet().iterator();
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
		}
		else
		{
			String current_key,current_value;
			Iterator<String> iter1=data.tailMap(leastHash).keySet().iterator();
			Iterator<String> iter2=data.headMap(low).keySet().iterator();
			if(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=data.get(current_key);
				result=result+current_key+","+current_value;
				data.remove(current_key,current_value);
			}
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=data.get(current_key);
				result=result+"_"+current_key+","+current_value;
				data.remove(current_key,current_value);
			}
			if(iter2.hasNext() && data.tailMap(leastHash).isEmpty())
			{
				current_key=iter2.next();
				current_value=data.get(current_key);
				result=result+current_key+","+current_value;
				data.remove(current_key,current_value);
			}
			while(iter2.hasNext())
			{
				current_key=iter2.next();
				current_value=data.get(current_key);
				result=result+"_"+current_key+","+current_value;
				data.remove(current_key,current_value);
			}
		}
		console.logExit();
		leastHash=low;
		return result;
	}
	
	public String newData(String newData)
	{
		console.logEntry();
		String[] insertions=newData.split("_");
		if(insertions[0].equals("")) return "OK";
		String[] current;
		int i;
		for(i=0;i<insertions.length;i++)
		{
			current=insertions[i].split(",");
			data.put(current[0],current[1]);
		}
		console.logExit();
		return "OK";
	}
	
	public String sendData()
	{
		console.logEntry();
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
		console.logExit();
		return result;
	}
	
	protected String action(String execute)
	{
		console.logEntry();
		String[] split=execute.split(",");
		String result;
		System.out.println("Action with: "+execute);
		if(split.length>1){
			if(split[1].equals("insert")) result="Answer-"+insert(split[0],split[2]);
			else if (split[1].equals("query")) result="Answer-"+query(split[0]);
			else if (split[1].equals("delete")) result="Answer-"+delete(split[0]);
			else result="Error";
		}
		else
		{
			split=execute.split("-");
			if (split[0].equals("NEWLOW")) result="BULK-"+changeRanges(split[1]);
			else if (split[0].equals("NEWLOW2")) result="NEWDATA-"+split[1]+"-"+sendData();
			else if (split[0].equals("NEWDATA")) 
			{
				leastHash=split[1];
				if(split.length>2) result=newData(split[2]);
				else result= "OK";
			}
			else if (split[0].equals("BULK")) 
			{
				if(split.length>1) result=newData(split[1]);
				else result= "OK";
			}
			else if (split[0].equals("LEAVE")) result="BULK-"+sendData();
			else result="Error";
		}
		console.logExit();
		return result;
	}
	
	public String insert(String key, String value)
	{
		console.logEntry();
		data.put(key,value);
		return "Done";
	}
	
	public String delete (String key)
	{
		console.logEntry();
		if(data.containsKey(key))
		{
			String value=data.remove(key);
			return value;
		}
		else return null;
	}
	
	public String query(String key)
	{
		console.logEntry();
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
			console.logExit();
			return result;
		}
		else
		{
			if(data.containsKey(key))
			{
				String current_value=data.get(key);
				result=key+","+current_value;
				console.logExit();
				return result;
			}
			console.logExit();
			return null;
		}
	}
	
}
