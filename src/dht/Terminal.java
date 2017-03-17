package dht;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Terminal {
	public int onePort;
	PrintStream out = System.out;
	private String [] cmds=
{"help", "exit","start", "ports","leave", "addnode", "insert","query", "delete","benchmark"};
	  
    private String [] options=
{"(for this message)", "","<k> 0|1|2 <onePort>", "","<port>", "<port>", "<key> <value> <port>",
     "<key> <port>", "<key> <port>","0|1|2"};
    
    public int k=1;
    public int rep=0;
    
    private String inputDirPath="inputs"+File.separator;
	private String [] files={"insert.txt","query.txt","requests.txt"};
	private String [] type={"insert,", "query,", ""};
    
	private ArrayList<Integer> ports=new ArrayList<Integer>();
	
	private void start() {
		Scanner sc=new Scanner(System.in);
		String s=null;
		while(true){
			out.print(">");
			s=sc.nextLine();
			String [] spl = s.split(" ");
			process(spl);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			out.println();
		}
	}
	
	private void process(String [] spl) {
	try{
		if(spl[0].equals("help")){
			helpMessage();
		}
		else if(spl[0].equals("exit")){
			System.exit(0);
		}
		else if(spl[0].equals("start")){
			onePort=4000;
			k=1;
			rep=0;
			if (spl.length>2){
				k=Integer.parseInt(spl[1]);
				rep=Integer.parseInt(spl[2]);
				onePort=Integer.parseInt(spl[3]);
			}
			if(k<1 || !(rep==0 || rep==1 || rep==2) || (rep!=0 && k==1))
				throw new Throwable();
			PrimaryRoutingServer prs;
			if(rep==0)
prs= new PrimaryRoutingServer("127.0.0.1",onePort);
			else
prs=new ReplicationPrimaryRoutingServer("127.0.0.1",onePort,k,rep);
			prs.start();
		}
		else if(spl[0].equals("leave")){
			simpleSend("LEAVE",Integer.parseInt(spl[1]));
			ports.remove(ports.indexOf(Integer.parseInt(spl[1])));
		}
		else if(spl[0].equals("addnode")){
			addNode(Integer.parseInt(spl[1]));
			ports.add(Integer.parseInt(spl[1]));
		}
		else if(spl[0].equals("status")){
			out.println("Primary is at "+onePort);
			out.println("k="+this.k);
			out.println("rep="+this.rep);
		}
		else if(spl[0].equals("adds")){
			Integer [] ps={2222,3333,4444,5555,6666,7777,8888,9999,4101};
			for (int p: ps){
				addNode(p);
				ports.add(p);
				
				Thread.sleep(1000);
			}
		}
		else if(spl[0].equals("addnodes")){
			for (int i=1;i<spl.length;i++){
				RoutingServer rs;
				int p=Integer.parseInt(spl[i]);
				addNode(p);
				ports.add(Integer.parseInt(spl[i]));
				
				Thread.sleep(1000);
			}
		}
		else if(spl[0].equals("insert")){
			String query=spl[1]+",insert,"+spl[2];
			send(query,Integer.parseInt(spl[3]));
		}
		else if(spl[0].equals("ports")){
			for(int p:ports)
				out.println(p);
		}
		else if(spl[0].equals("query")){
			String query=spl[1]+",query";
			send(query,Integer.parseInt(spl[2]));
		}
		else if(spl[0].equals("delete")){
			String query=spl[1]+",delete";
			send(query,Integer.parseInt(spl[2]));
		}
		else if(spl[0].equals("benchmark")){
			benchmark(spl);
		}
		else
			throw new Throwable();
	}catch (Throwable e){
		e.printStackTrace();
		System.out.println("Invalid command");
		helpMessage();
	}
	}
	
	private void benchmark(String[] spl) throws IOException {
		int test=Integer.parseInt(spl[1]);
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		long starting_time=timestamp.getTime();
		List<String> list = Files.readAllLines(Paths.get(inputDirPath + files[test]));
		String [] lines = list.toArray(new String[list.size()]);
		for(int i=0; i<lines.length; i++){
			String [] ss=lines[i].split(",");
			for(int j=0; j<ss.length; j++)
				ss[j]=ss[j].trim();
		switch(test){ 
		case 0: lines[i]=ss[0]+",insert,"+ss[1];
			break;
		case 1: lines[i]=ss[0]+",query";
			break;
		case 2: 
			lines[i]=ss[1]+","+ss[0];
			if(ss.length==3)
				lines[i]+=","+ss[2];
			break;
		}
		}
		
		//establish connections
		int nodes=ports.size();
		Socket [] socket=new Socket[nodes];
		PrintWriter [] pw=new PrintWriter[nodes];
		BufferedReader [] br=new BufferedReader[nodes];
		for (int i=0;i<nodes;i++){
			socket[i]=new Socket("127.0.0.1",ports.get(i));
			pw[i] = new PrintWriter(socket[i].getOutputStream(), true);
			InputStreamReader inputstream=new InputStreamReader(socket[i].getInputStream());
			br[i]=new BufferedReader(inputstream);
		}
		
		Integer [] randoms=new Integer[lines.length];
		for (int i=0; i<lines.length; i++){
			int r=ThreadLocalRandom.current().nextInt(0, nodes);//select in [0,1,...,nodes-1]
			randoms[i]=r;
		}
		
		for (int i=0; i<lines.length; i++){
			out.println("&&&&& sending "+lines[i]+" to "+ports.get(randoms[i]));
			pw[randoms[i]].println(lines[i]);
			String reply=br[randoms[i]].readLine();
			out.println("->"+lines[i]);
			out.println("->"+reply);
		}
		timestamp = new Timestamp(System.currentTimeMillis());
		starting_time=timestamp.getTime()-starting_time;
		out.println("Run time = "+starting_time+" milliseconds");
	}

	private void simpleSend(String mess, int port) {
	try{
		Socket sock = new Socket("127.0.0.1",port);
		PrintWriter pw=new PrintWriter(sock.getOutputStream(), true);
		pw.println(mess);
		sock.close();
	} catch (UnknownHostException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}	
	
	}

	private void addNode(int myPort){
		RoutingServer rs=null;
		switch (rep) {
		case 0:
rs=new RoutingServer("127.0.0.1",myPort,"127.0.0.1",onePort);
break;
		case 1:
rs=new LinearizationRoutingServer("127.0.0.1",myPort,"127.0.0.1",onePort,k);
break;
		case 2:
rs=new EventualRoutingServer("127.0.0.1",myPort,"127.0.0.1",onePort,k);
break;
		}
	rs.start();
	}
	
	private void send(String query, int port){
		Socket CltSocket;
		try {
			CltSocket = new Socket("127.0.0.1",port);
			PrintWriter pw=new PrintWriter(CltSocket.getOutputStream(), true);
			pw.println(query);
			BufferedReader br = new BufferedReader(
	                new InputStreamReader(CltSocket.getInputStream()));
			String master =br.readLine();
			Thread.sleep(1000);
			if (!master.contains("_"))
				System.out.println(master);
			else{
                //      result of a query * with many answers
				String [] spl=master.split("_");
				for (String s:spl)
					out.println(s);
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

	private void helpMessage(){
		for(int i=0; i<cmds.length; i++){
			System.out.println(cmds[i]+"\t"+options[i]);
		}
	}
	
	public static void main(String [] args){
		Terminal t = new Terminal();
		t.start();
	}

}
