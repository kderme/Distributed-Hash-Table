package dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Terminal {
	public int port; 
	PrintStream out = System.out;
	private String [] cmds=
{"help", "exit","start", "leave", "addnode", "insert","query", "delete","benchmark"};
	  
    private String [] options=
{"(for this message)", "","<port> <k> 0|1|2", "<port>", "<port>", "<key> <value> <port>",
     "<key> <port>", "<key> <port>","1|2|3"};
    
    public int k=1;
    public int rep=0;
    
    private String inputDirPath="input/";
	private String [] files={"insert.txt","query.txt","request.txt"};
	private String [] type={"insert,", "query,", ""};
    
	private ArrayList<Integer> ports=new ArrayList<Integer>(10);
	
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
			int port=4000;
			k=1;
			rep=0;
			if (spl.length>1){
				port=Integer.parseInt(spl[1]);
			}
			if (spl.length>2){
				k=Integer.parseInt(spl[2]);
				rep=Integer.parseInt(spl[3]);
			}
			if(k<1 || !(rep==0 || rep==1 || rep==2) || (rep==0 && k!=1)){
				throw new Throwable();
			}
			RoutingServer prs;
			if(rep==0)
prs= new PrimaryRoutingServer("127.0.0.1",1111,"127.0.0.1",port);
			else
prs=new ReplicationPrimaryRoutingServer("127.0.0.1",1111,"127.0.0.1",port,k,rep);
			prs.start();
			new Thread(prs).start();
			ports.add(1111);
		}
		else if(spl[0].equals("leave")){
			simpleSend("Leave",Integer.parseInt(spl[1]));
		}
		else if(spl[0].equals("addnode")){
			RoutingServer rs;
			if(rep==0)
rs= new RoutingServer("127.0.0.1",Integer.parseInt(spl[1]),"127.0.0.1",4000);
			else
rs=new ReplicationRoutingServer("127.0.0.1",Integer.parseInt(spl[1]),"127.0.0.1",4000,k,rep);
			rs.start();
			ports.add(Integer.parseInt(spl[1]));
		}
		else if(spl[0].equals("insert")){
			String query=spl[1]+",insert,"+spl[2];
			send(query,Integer.parseInt(spl[3]));
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
		List<String> list = Files.readAllLines(Paths.get(inputDirPath + files[test]));
		String [] lines = list.toArray(new String[list.size()]);
		for(int i=0; i<lines.length; i++){
			String [] ss=lines[i].split(",");
			for(int j=0; j<ss.length; j++)
				ss[j]=ss[j].trim();
		switch(test){ 
		case 0: lines[i]=ss[0]+",insert,"+ss[1];
			break;
		case 1: lines[i]=ss[0]+",querry";
			break;
		case 2: 
			lines[i]=ss[1]+","+ss[0];
			if(ss.length==3)
				lines[i]+=ss[2];
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
			pw[randoms[i]].println(lines[i]);
			String reply=br[randoms[i]].readLine();
			out.println("->"+lines[i]);
			out.println("->"+reply);
		}

		
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
			System.out.println(master);
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
