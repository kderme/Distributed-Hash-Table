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
	public Server(boolean master,String least,String max)
	{
		data=new TreeMap<String,String>();
		isMaster=master;
		leastHash=least;
		maxHash=max;
	}
	public String hashString(String value)
	{
		return value;//not used yet
	}

	public String changeRanges(String low)
	{
		leastHash=low;
		String result="";
		String current_key,current_value;
		Iterator<String> iter=data.tailMap(maxHash).keySet().iterator();
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
	
	public String newData(String newData)
	{
		String[] insertions=newData.split("_");
		String[] current;
		int i;
		for(i=0;i<insertions.length;i++)
		{
			current=insertions[i].split(",");
			data.put(current[0],current[1]);
		}
		return "OK";
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
	
	//kaleis thn action me ta ekshs Strings
	//gia eisagwgh kombou NewData-key,value_key,value_key,value_.....
	//apantaei OK
	//gia eksagwgh kombou Leaving
	//apantaei Data-key,value_key,value_key,value_.....
	//gia allagh ranges NewRange-lowRange
	//apantaei Data-key,value_key,value_key,value_.....
	
	protected String action(String execute)
	{
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
			else if (split[0].equals("BULK")) 
			{
				if(split.length>1) result=newData(split[1]);
				else result= "OK";
			}
			else if (split[0].equals("LEAVE")) result="BULK-"+sendData();
			else result="Error";
		}
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
