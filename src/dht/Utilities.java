package dht;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class Utilities {
	
	public static void init(){
		
	}
	
	public static int compareHash(String h1,String h2){
		for(int i=0;i<40;i++){
			int d=h1.charAt(i)-h2.charAt(i);
			if(d!=0)
			return d;
		}
		return 0;
	}

	protected static boolean sendMessage(String ip, int port, String message,Console console){
        console.log("Sending message: "+message+"to: "+ip+":"+port);
        while(true){
                try{
                        Socket socket = new Socket(ip,port);
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        out.println(message);
                        socket.close();
                        return true;
                } catch(IOException e){
                        console.log("can`t send to "+port);
                        continue;
                }
        }
	}
	
	protected static String sendMessageWithReply(String ip, int port, String message,Console console){
		console.log("Sending message: "+message);
		console.log("to: "+ip+":"+port);
		try{
			Socket socket = new Socket(ip,port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader inOne= new BufferedReader(
	                new InputStreamReader(socket.getInputStream()));
			out.println(message);
			String reply=inOne.readLine();
			console.log("This is the reply: "+reply);
			socket.close();
			return reply;
		} catch(IOException e){
			e.printStackTrace();
			return "";
		}
	}

	protected static boolean sendMessage(String iPort,String message,Console console){
		
		String next []=iPort.split(":");
		
		return sendMessage(next[0],new Integer(next[1]),message,console);
	}
		
	protected static void sendMessage(String Ip, String port, String message, String errorMessage,Console console)
	{
		console.logEntry();
		console.log("Sending message: "+message);
		console.log("to: "+Ip+":"+port);
		try{	
			Socket CltSocket=new Socket(Ip,Integer.parseInt(port));
			new PrintWriter(CltSocket.getOutputStream(), true).println(message);
			CltSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
			console.log(errorMessage);
			System.exit(1);
		}
		console.logExit();
	}
	public static String hash(String s){
		return org.apache.commons.codec.digest.DigestUtils.sha1Hex(s);
	}
	
	public static String hash(int n){
		return hash(n+"");
	}
}