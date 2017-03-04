package dht;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

public class ReplicationServer extends Server{
	private int replicaNumber;
	private String replicaLeastHash;
	private String replicaMaxHash;
	SortedMap<String,String> replicaData = null;
	SortedMap<String,String> replicaWriteTime = null;
	private int replicationMethod;
	public ReplicationServer(boolean master,String least,String max, int replicas, String replicalow, String replicahigh, int replicaMethod)
	{
		super(master,least,max);
		replicaNumber=replicas;
		replicaLeastHash=replicalow;
		replicaMaxHash=replicahigh;
		replicationMethod=replicaMethod;
	}
	
	private boolean isReplicaMaster(String key)
	{
		boolean result=true;
		if(RoutingServer.compareHash(leastHash,maxHash)<0)
		{
			result=result && (RoutingServer.compareHash(leastHash,key)<0);
			result=result && (RoutingServer.compareHash(maxHash,key)>=0);
		}
		else
		{
			result=result && (RoutingServer.compareHash(leastHash,key)<0);
			result=result || (RoutingServer.compareHash(maxHash,key)>=0);
		}
		return result;
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
	
	private String removeReplicas(String lowReplica, String destination)
	{
		String result=destination+"-";
		String marginLow=replicaLeastHash;
		String marginHigh=lowReplica;
		replicaLeastHash=lowReplica;
		Iterator<String> iter=replicaData.subMap(marginLow,marginHigh).keySet().iterator();
		if(iter.hasNext())
		{
			String key=iter.next();
			String value=replicaData.get(key);
			result=result+key+","+value;
			replicaData.remove(key);
		}
		while(iter.hasNext())
		{
			String key=iter.next();
			String value=replicaData.get(key);
			result=result+"_"+key+","+value;
			replicaData.remove(key);
		}
		return result;
	}
	
	private String sendAllData(String replicaLow, String destination)
	{
		String result=destination+"-";
		String marginLow=replicaLeastHash;
		Iterator<String> iter=replicaData.keySet().iterator();
		String key,value;
		if(iter.hasNext())
		{
			key=iter.next();
			value=replicaData.get(key);
			result=result+key+","+value;
			replicaData.remove(key);
		}
		while(iter.hasNext())
		{
			key=iter.next();
			value=replicaData.get(key);
			result=result+"_"+key+","+value;
			replicaData.remove(key);
		}
		iter=data.keySet().iterator();
		if(replicaData.isEmpty())
		{
			if(iter.hasNext())
			{
				key=iter.next();
				value=data.get(key);
				result=result+key+","+value;
			}
		}
		while(iter.hasNext())
		{
			key=iter.next();
			value=data.get(key);
			result=result+"_"+key+","+value;
		}
		return result;
	}
	
	private String updateReplica(String dataString)
	{
		String result="";
		return result;
	}
	
	public String action(String execute)
	{
		String[] split=execute.split(",");
		String result;
		if(split[1].equals("insert")) result="ANSWER-"+insert(split[0],split[2]);
		else if (split[1].equals("query")) result="ANSWER-"+query(split[0]);
		else if (split[1].equals("delete")) result="ANSWER-"+delete(split[0]);
		else if (split[0].equals("NewRange")) result="BULK-"+changeRanges(split[1]);
		else if (split[0].equals("BULK")) result=newData(split[1]);
		else if (split[0].equals("NEWREPLICALOW")) result="UPDATEREPLICA-"+removeReplicas(split[1],split[2]);
		else if (split[0].equals("Leaving")) result="BULK-"+sendData();
		else if (split[0].equals("SENDALLDATA")) result="UPDATEREPLICA-"+sendAllData(split[1],split[2]);
		else if (split[0].equals("UPDATEREPLICA")) result=updateReplica(split[2]);
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
