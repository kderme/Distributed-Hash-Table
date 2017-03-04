package client;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

public class Client {
		//default args
		private String ip="127.0.0.1";
		private int port=4000;
		private String inputDirPath="input/";
		private String [] files={"insert.txt","querry.txt","request.txt"};
		private String [] type={"insert,", "querry,", ""};
		
		private int nodes=10;
		
		public void run(int test) throws IOException{
			//read file and process
			List<String> list = Files.readAllLines(Paths.get(inputDirPath + files[test]));
			String [] lines = list.toArray(new String[list.size()]);
			for(int i=0; i<lines.length; i++){
				String [] ss=lines[i].split(",");
				for(int j=0; j<lines.length; j++)
					ss[j]=ss[j].trim();
				switch(test){
				case 0: lines[i]=hash(ss[0])+",insert,"+ss[1];
					break;
				case 1: lines[i]=hash(ss[0])+",querry";
					break;
				case 2: 
					lines[i]=hash(ss[1])+","+ss[0];
					if(ss.length==3)
						lines[i]+=ss[2];
					break;
				}
			}

			//establish connections
			Socket [] socket=new Socket[nodes];
			PrintWriter [] pw=new PrintWriter[nodes];
			BufferedReader [] br=new BufferedReader[nodes];
			for (int i=0;i<nodes;i++){
				socket[i]=new Socket(ip,port+i+1);
				pw[i] = new PrintWriter(socket[i].getOutputStream(), true);
				InputStreamReader inputstream=new InputStreamReader(socket[i].getInputStream());
				br[i]=new BufferedReader(inputstream);
			}

			//random ports
			ArrayList<String> [] ls=new ArrayList[nodes];
			Integer [] randoms=new Integer[lines.length];
			for (int i=0; i<randoms.length; i++){
				int r=ThreadLocalRandom.current().nextInt(0, nodes);//select in [0,1,...,nodes-1]
				randoms[i]=r;
				ls[r].add(lines[i]);
			}

			//send queries
			for(int i=0; i<lines.length; i++)
				pw[randoms[i]].println(lines[i]);
			
			//close
			for (int i=0;i<nodes;i++){
				br[i].close();
				pw[i].close();
				socket[i].close();
			}
		}
		
		public void start(int policy){
			try{
			switch (policy){
			case 0: 
				run(0);
				break;
				
			case 1: 
				run(0);
				Thread.sleep(5000);	//sleep 5 seconds
				run(1);
			
			case 2:
				run(2);
			}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} 

		}
		
		public void main(String [] args){
			int policy=0;
			if (args.length == 0) {}
			else if (args.length == 2 || args.length == 4 || args.length == 6|| args.length == 8) {
				for (int i=0; i<args.length; i+=2){	
				if (args[i].equals("-ip"))
					ip=args[i+1];
				else if (args[i].equals("-p"))
					port=Integer.parseInt(args[i+1]);
				else if (args[i].equals("-f"))
					files=args[i+1].split(",");
				else if (args[i].equals("-t"))
					policy=Integer.parseInt(args[i+1]);
				else
					printUsage();
				}
			}
			else
				printUsage();

			start(policy);
		}
		
		private void printUsage(){
			String usage="java [-t test_number] [-ip ip] [-p port] [-f file1,file2,file3,...]"; 
			System.out.println("Usage"+usage);
			System.out.println("flags are optional and can be used in any order");
			System.exit(1);
		}
		
		public static String hash(String s){
			return org.apache.commons.codec.digest.DigestUtils.sha1Hex("");
		}
}
