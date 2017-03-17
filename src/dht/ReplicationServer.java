package dht;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ReplicationServer extends Server{
	private String replicaLeastHash;
	SortedMap<String,String> replicaData = null;
	SortedMap<String,String> WriteTime = null;
	public ReplicationServer(boolean master,String least,String max, int replicas, String replicalow, String replicahigh, int replicaMethod, Console routingConsole)
	{
		super(master,least,max,routingConsole);
		replicaLeastHash=replicalow;
		replicaData=new TreeMap<String,String>();
		WriteTime=new TreeMap<String,String>();
	}
	
	private boolean isReplicaMaster(String key)
	{
		console.logEntry();
		boolean result=true;
		if(Utilities.compareHash(leastHash,maxHash)<0)
		{
			result=result && (Utilities.compareHash(leastHash,key)<=0);
			result=result && (Utilities.compareHash(maxHash,key)>0);
		}
		else
		{
			result=result && (Utilities.compareHash(leastHash,key)<=0);
			result=result || (Utilities.compareHash(maxHash,key)>0);
		}
		console.logExit();
		return result;
	}
	
	public String changeRanges(String low)
	{
		console.logEntry();
		String result="";
		if(Utilities.compareHash(low,leastHash)>=0)
		{
			String current_key,current_value;
			console.log("one Margin low: "+leastHash+"\nMarginHigh: "+low);
			Iterator<String> iter=data.subMap(leastHash,low).keySet().iterator();
			ArrayList<String> temp=new ArrayList<String>();
			if(iter.hasNext())
			{
				current_key=iter.next();
				current_value=data.get(current_key);
				temp.add(current_key);
				replicaData.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+current_key+","+current_value;
			}
			while(iter.hasNext())
			{
				current_key=iter.next();
				current_value=data.get(current_key);
				temp.add(current_key);
				replicaData.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+"_"+current_key+","+current_value;
			}
			iter=temp.iterator();
			while(iter.hasNext())
			{
				current_key=iter.next();
				data.remove(current_key);
			}
		}
		else
		{
			String current_key,current_value;
			console.log("two Margin low: "+leastHash+"\nMarginHigh: "+low);
			Iterator<String> iter1=data.tailMap(leastHash).keySet().iterator();
			Iterator<String> iter2=data.headMap(low).keySet().iterator();
			ArrayList<String> temp=new ArrayList<String>();
			if(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=data.get(current_key);
				temp.add(current_key);
				replicaData.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+current_key+","+current_value;
			}
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=data.get(current_key);
				temp.add(current_key);
				replicaData.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+"_"+current_key+","+current_value;
			}
			if(iter2.hasNext() && data.tailMap(leastHash).isEmpty())
			{
				current_key=iter2.next();
				current_value=data.get(current_key);
				temp.add(current_key);
				replicaData.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+current_key+","+current_value;
			}
			while(iter2.hasNext())
			{
				current_key=iter2.next();
				current_value=data.get(current_key);
				temp.add(current_key);
				replicaData.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+"_"+current_key+","+current_value;
			}
			iter1=temp.iterator();
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				data.remove(current_key);
			}
		}
		console.logExit();
		leastHash=low;
		return result;
	}
	
	public String replicaToData(String low)
	{
		console.logEntry();
		String result=low+"-";
		String current_key,current_value;
		ArrayList<String> temp=new ArrayList<String>();
		if(Utilities.compareHash(low,leastHash)<=0)
		{
			Iterator<String> iter=replicaData.subMap(low,leastHash).keySet().iterator();
			if(iter.hasNext())
			{
				current_key=iter.next();
				current_value=replicaData.get(current_key);
				temp.add(current_key);
				data.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+current_key+","+current_value;
			}
			iter=replicaData.subMap(low,leastHash).keySet().iterator();
			while(iter.hasNext())
			{
				current_key=iter.next();
				current_value=replicaData.get(current_key);
				temp.add(current_key);
				data.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+"_"+current_key+","+current_value;
			}
			iter=temp.iterator();
			while(iter.hasNext())
			{
				current_key=iter.next();
				replicaData.remove(current_key);
			}
		}
		else
		{
			Iterator<String> iter1=replicaData.tailMap(low).keySet().iterator();
			boolean flag=replicaData.tailMap(low).isEmpty();
			if(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=replicaData.get(current_key);
				temp.add(current_key);
				data.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+current_key+","+current_value;
			}
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=replicaData.get(current_key);
				temp.add(current_key);
				data.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+"_"+current_key+","+current_value;
			}
			Iterator<String> iter2=replicaData.headMap(leastHash).keySet().iterator();
			if(iter2.hasNext() && flag)
			{
				current_key=iter2.next();
				current_value=replicaData.get(current_key);
				temp.add(current_key);
				data.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+current_key+","+current_value;
			}
			while(iter2.hasNext())
			{
				current_key=iter2.next();
				current_value=replicaData.get(current_key);
				temp.add(current_key);
				data.put(current_key,current_value);
				//*********************************
				current_value=current_value+"%"+WriteTime.get(current_key);
				//*********************************
				result=result+"_"+current_key+","+current_value;
			}
			iter1=temp.iterator();
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				replicaData.remove(current_key);
			}
		}
		leastHash=low;
		console.logExit();
		return result;
	}
	
	public String newData(String newData)
	{
		console.logEntry();
		String[] insertions=newData.split("_");
		String[] current;
		if(insertions[0].equals("")) return "OK";
		int i;
		for(i=0;i<insertions.length;i++)
		{
			current=insertions[i].split(",");
			//*********************************
			console.log("HERE "+current.length+" "+current[1]+" "+current[1].split("%")[0]);
			String current_value=current[1].split("%")[1];
			current[1]=current[1].split("%")[0];
			WriteTime.put(current[0],current_value);
			//*********************************
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
			//*********************************
			String current_time=WriteTime.get(current_key);
			result=result+"%"+current_time;
			//*********************************
		}
		while(iter.hasNext())
		{
			current_key=iter.next();
			current_value=data.get(current_key);
			result=result+"_"+current_key+","+current_value;
			//*********************************
			String current_time=WriteTime.get(current_key);
			result=result+"%"+current_time;
			//*********************************
		}
		console.logExit();
		return result;
	}
	
	
	private String removeReplicas(String lowReplica, String destination)
	{
		console.logEntry();
		ArrayList<String> temp=new ArrayList<String>();
		String result=replicaLeastHash+"-"+destination+"-";
		String marginLow=replicaLeastHash;
		String marginHigh=lowReplica;
		replicaLeastHash=lowReplica;
		console.log("marginLow:"+marginLow+"\nmarginHigh:"+marginHigh);
		if(Utilities.compareHash(marginLow,marginHigh)<0)
		{
			Iterator<String> iter=replicaData.subMap(marginLow,marginHigh).keySet().iterator();
			if(iter.hasNext())
			{
				String key=iter.next();
				String value=replicaData.get(key);
				result=result+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				WriteTime.remove(key);
				//*********************************
				temp.add(key);
			}
			while(iter.hasNext())
			{
				String key=iter.next();
				String value=replicaData.get(key);
				result=result+"_"+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				WriteTime.remove(key);
				//*********************************
				temp.add(key);
			}
			iter=temp.iterator();
			while(iter.hasNext())
			{
				String key=iter.next();
				replicaData.remove(key);
			}
		}
		else
		{
			Iterator<String> iter1=replicaData.tailMap(marginLow).keySet().iterator();
			Iterator<String> iter2=replicaData.headMap(marginHigh).keySet().iterator();
			if(iter1.hasNext())
			{
				String key=iter1.next();
				String value=replicaData.get(key);
				result=result+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				WriteTime.remove(key);
				//*********************************
				temp.add(key);
			}
			while(iter1.hasNext())
			{
				String key=iter1.next();
				String value=replicaData.get(key);
				result=result+"_"+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				WriteTime.remove(key);
				//*********************************
				temp.add(key);
			}
			if(iter2.hasNext())
			{
				String key=iter2.next();
				String value=replicaData.get(key);
				result=result+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				WriteTime.remove(key);
				//*********************************
				temp.add(key);
			}
			while(iter2.hasNext())
			{
				String key=iter2.next();
				String value=replicaData.get(key);
				result=result+"_"+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				WriteTime.remove(key);
				//*********************************
				temp.add(key);
			}
			iter1=temp.iterator();
			while(iter1.hasNext())
			{
				String key=iter1.next();
				replicaData.remove(key);
			}
		}
		console.logExit();
		return result;
	}
	
	private String sendAllData(String replicaLow, String destination)
	{
		console.logEntry();
		String result=replicaLow+"-"+destination+"-";
		Iterator<String> iter=replicaData.keySet().iterator();
		String key,value;
		if(iter.hasNext())
		{
			key=iter.next();
			value=replicaData.get(key);
			result=result+key+","+value;
			//*********************************
			String current_time=WriteTime.get(key);
			result=result+"%"+current_time;
			//WriteTime.remove(key);
			//*********************************

			//replicaData.remove(key);
		}
		while(iter.hasNext())
		{
			key=iter.next();
			value=replicaData.get(key);
			result=result+"_"+key+","+value;
			//*********************************
			String current_time=WriteTime.get(key);
			result=result+"%"+current_time;
			//WriteTime.remove(key);
			//*********************************
			
			//replicaData.remove(key);
		}
		iter=data.keySet().iterator();
		if(replicaData.isEmpty())
		{
			if(iter.hasNext())
			{
				key=iter.next();
				value=data.get(key);
				result=result+key+","+value;
				//*********************************
				String current_time=WriteTime.get(key);
				result=result+"%"+current_time;
				//*********************************
				
			}
		}
		while(iter.hasNext())
		{
			key=iter.next();
			value=data.get(key);
			result=result+"_"+key+","+value;
			//*********************************
			String current_time=WriteTime.get(key);
			result=result+"%"+current_time;
			//*********************************
			
		}
		console.logExit();
		return result;
	}
	
	private String updateReplica(String low,String dataString)
	{
		console.logEntry();
		String result="";
		//if(RoutingServer.compareHash(low,replicaLeastHash)<0) replicaLeastHash=low;
		String[] dataPairs=dataString.split("_");
		if(dataPairs[0].equals("")) return result;
		int i;
		String key,value;
		for (i=0; i<dataPairs.length;i++)
		{
			key=dataPairs[i].split(",")[0];
			value=dataPairs[i].split(",")[1];
			if (isReplicaMaster(key)){
				//*********************************
				String current_time=value.split("%")[1];
				value=value.split("%")[0];
				WriteTime.put(key,current_time);
				//*********************************
				data.put(key,value);
			}
			else {
				//*********************************
				String current_time=value.split("%")[1];
				value=value.split("%")[0];
				WriteTime.put(key,current_time);
				//*********************************
				replicaData.put(key,value);
			}
		}
		console.logExit();
		return result;
	}
	private String addReplica (String low, String dataString)
	{
		console.logEntry();
		replicaLeastHash=low;
		updateReplica(low,dataString);
		console.logExit();
		return "";
	}
	
	private String sendReplica(String low, String high)
	{
		console.logEntry();
		ArrayList<String> temp=new ArrayList<String>();
		String result="";
		if(Utilities.compareHash(low,high)<=0)
		{
			String current_key,current_value;
			Iterator<String> iter=replicaData.subMap(low,high).keySet().iterator();
			if(iter.hasNext())
			{
				current_key=iter.next();
				current_value=replicaData.get(current_key);
				result=result+current_key+","+current_value;
				//*********************************
				String current_time=WriteTime.get(current_key);
				result=result+"%"+current_time;
				WriteTime.remove(current_key);
				//*********************************
				temp.add(current_key);
			}
			while(iter.hasNext())
			{
				current_key=iter.next();
				current_value=replicaData.get(current_key);
				result=result+"_"+current_key+","+current_value;
				//*********************************
				String current_time=WriteTime.get(current_key);
				result=result+"%"+current_time;
				WriteTime.remove(current_key);
				//*********************************
				temp.add(current_key);
			}
			iter=temp.iterator();
			while(iter.hasNext())
			{
				current_key=iter.next();
				replicaData.remove(current_key);
			}
			
		}
		else
		{
			String current_key,current_value;
			Iterator<String> iter1=replicaData.tailMap(low).keySet().iterator();
			Iterator<String> iter2=replicaData.headMap(high).keySet().iterator();
			boolean flag=replicaData.tailMap(low).isEmpty();
			if(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=replicaData.get(current_key);
				result=result+current_key+","+current_value;
				//*********************************
				String current_time=WriteTime.get(current_key);
				result=result+"%"+current_time;
				WriteTime.remove(current_key);
				//*********************************
				temp.add(current_key);
			}
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				current_value=replicaData.get(current_key);
				result=result+"_"+current_key+","+current_value;
				//*********************************
				String current_time=WriteTime.get(current_key);
				result=result+"%"+current_time;
				WriteTime.remove(current_key);
				//*********************************
				temp.add(current_key);
			}
			if(iter2.hasNext() && flag)
			{
				current_key=iter2.next();
				current_value=replicaData.get(current_key);
				result=result+current_key+","+current_value;
				//*********************************
				String current_time=WriteTime.get(current_key);
				result=result+"%"+current_time;
				WriteTime.remove(current_key);
				//*********************************
				temp.add(current_key);
			}
			while(iter2.hasNext())
			{
				current_key=iter2.next();
				current_value=replicaData.get(current_key);
				result=result+"_"+current_key+","+current_value;
				//*********************************
				String current_time=WriteTime.get(current_key);
				result=result+"%"+current_time;
				WriteTime.remove(current_key);
				//*********************************
				temp.add(current_key);
			}
			iter1=temp.iterator();
			while(iter1.hasNext())
			{
				current_key=iter1.next();
				replicaData.remove(current_key);
			}
		}
		console.logExit();
		return result;
	}
	
	protected String action(String execute)
	{
		console.logEntry();
		String[] split=execute.split("-");
		String result;
		if(split.length==1){
			split=execute.split(",");
			if(split[1].equals("insert")) result=insert(split[0],split[2]);
			else if (split[1].equals("query")) result=query(split[0]);
			else if (split[1].equals("delete")) result=delete(split[0]);
			else result="Error";
		}
		else
		{
			split=execute.split("-");
			if (split[0].equals("NEWLOW")) result="BULK-"+changeRanges(split[1]);
			else if (split[0].equals("NEWLOW2")) result="UPDATEREPLICA-"+replicaToData(split[1]);
			else if (split[0].equals("BULK")) {
				if(split.length==1) result=newData("");
				else result=newData(split[1]);
			}
			else if (split[0].equals("NEWREPLICALOW")) result="UPDATEREPLICA-"+removeReplicas(split[1],split[2]);
			else if (split[0].equals("LEAVE")) result="BULK-"+sendData();
			else if (split[0].equals("SENDALLDATA")) result="ADDREPLICA-"+sendAllData(split[1],split[2]);
			else if (split[0].equals("SENDREPLICA")) result="UPDATEREPLICA-"+split[1]+"-"+split[3]+"-"+sendReplica(split[1],split[2]);
			else if (split[0].equals("SENDDATA")) result="ADDREPLICA-"+split[1]+"-"+split[2]+"-"+sendData();
			else if (split[0].equals("ADDREPLICA")) {
				if(split.length==3)	result=addReplica(split[1],"");
				else result=addReplica(split[1],split[3]);
			}
			else if (split[0].equals("UPDATEREPLICA")) {
				if(split.length<=3) result=updateReplica(split[1],"");
				else result=updateReplica(split[1],split[3]);
			}
			else result="Error";
		}
		console.logExit();
		return result;
	}
	
	public String insert(String key, String value)
	{
		//*********************************
		console.logEntry();
		Timestamp timestamp=new Timestamp(System.currentTimeMillis());
		String current_time=WriteTime.get(key);
		long writing_time;
		long time;
		if(value.split("%").length>1){
			writing_time=Long.parseLong(value.split("%")[1]);
			value=value.split("%")[0];
		}
		else writing_time=timestamp.getTime();
		if(current_time==null) time=0;
		else time=Long.valueOf(current_time);
		if(time>writing_time) return "NotDone%"+writing_time;
		WriteTime.put(key,""+writing_time);
		//*********************************
		if(isReplicaMaster(key))
		{
			data.put(key,value);
		}
		else
		{
			replicaData.put(key,value);
		}
		console.logExit();
		return "Done%"+writing_time;
	}
	
	public String delete (String key)
	{
		console.logEntry();
		Timestamp timestamp=new Timestamp(System.currentTimeMillis());
		String current_time=WriteTime.get(key);
		long writing_time;
		long existing_time;
		if(key.split("%").length>1){
			writing_time=Long.parseLong(key.split("%")[1]);
			key=key.split("%")[0];
		}
		else writing_time=timestamp.getTime();
		if(current_time==null) existing_time=0;
		else existing_time=Long.valueOf(current_time);
		if(existing_time>writing_time) return "NotDone%"+writing_time;
		WriteTime.put(key,""+writing_time);
		if(data.containsKey(key))
		{
			String value=data.remove(key);
			//**************************
			value=value+"%"+writing_time;
			//**************************
			console.logExit();
			return value;
		}
		else if(replicaData.containsKey(key))
		{
			String value=replicaData.remove(key);
			//**************************
			value=value+"%"+writing_time;
			//**************************
			console.logExit();
			return value;
		}
		else return "NotFound%"+writing_time;
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
				//**************************
				String time=WriteTime.get(current_key);
				if (time!=null) current_value=current_value+"%"+time;
				//**************************
				result=result+current_key+","+current_value;
			}
			while(iter.hasNext())
			{
				current_key=(String) iter.next();
				current_value=data.get(current_key);
				//**************************
				String time=WriteTime.get(current_key);
				if (time!=null) current_value=current_value+"%"+time;
				//**************************
				result=result+"_"+current_key+","+current_value;
			}
			console.logExit();
			return result;
		}
		else
		{
			if(data.containsKey(key))
			{
				String current_value=data.get(key);
				//**************************
				String time=WriteTime.get(key);
				if (time!=null) current_value=current_value+"%"+time;
				//**************************
				result=key+","+current_value;
				console.logExit();
				return result;
			}
			else if(replicaData.containsKey(key))
			{
				String current_value=replicaData.get(key);
				//**************************
				String time=WriteTime.get(key);
				if (time!=null) current_value=current_value+"%"+time;
				//**************************
				result=key+","+current_value;
				console.logExit();
				return result;
			}
			console.logExit();
			return null;
		}
	}
}
