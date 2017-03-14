package dht;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class Console {
	public PrintStream out=System.out;
	public String iport="";
	public boolean writeToFile=false;
	private PrintStream outFile;
	
	public Console(){
	}
	
	public Console(String iport){
		this.iport=iport;
	}
	
	public Console(String iport,String logFileName){
		this.iport=iport;
		try {
			PrintStream out = new PrintStream(
				     new FileOutputStream(logFileName));
			this.outFile=out;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			log(logFileName+ "din`t open. Log file is stdout now");
		} 
		writeToFile=true;
	}
	
	public void logEntry(){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		String st="["+iport+"/"+caller.getMethodName()+"]    "+"entry";
		prints(st);
	}
	
	public void logExit(){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		String st="["+iport+"/"+caller.getMethodName()+"]    "+"exit";
		prints(st);
	}
	
	public void log(Object s){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		String st="["+iport+"/"+caller.getMethodName()+"]    "+s;
		prints(st);
	}
	 
	public void prints(String s){
		out.println(s);
		if(writeToFile){
			outFile.println(s);
		}
	}
}