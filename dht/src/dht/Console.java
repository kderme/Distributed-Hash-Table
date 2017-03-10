package dht;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class Console {
	public PrintStream out=System.out;
	
	public Console(){
	}
	
	public Console(String logFileName){
		try {
			PrintStream out = new PrintStream(
				     new FileOutputStream(logFileName));
			this.out=out;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.out.println(logFileName+ "din`t open. Log file is stdout now");
		} 
	}
	
	public void logEntry(){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		out.println("["+caller.getMethodName()+"]    "+"entry");
	}
	
	public void logExit(){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		out.println("["+caller.getMethodName()+"]    "+"exit");
	}
	
	public void logEntry(String iport){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		out.println("["+iport+"/"+caller.getMethodName()+"]    "+"entry");
	}
	
	public void logExit(String iport){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		out.println("["+iport+"/"+caller.getMethodName()+"]    "+"exit");
	}
	
	public void log(Object s){
		StackTraceElement caller = new Throwable().getStackTrace()[1];
		out.println("["+caller.getMethodName()+"]    "+s);
	}
}