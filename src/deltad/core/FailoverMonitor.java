package deltad.core;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import sun.tools.jconsole.LocalVirtualMachine;

public class FailoverMonitor {
	//Logger logger = Logger.getLogger(FailoverMonitor.class.getName());
	private final String server;
	private final String server_cmd;
	public static String source_agent_cmd = "deltad.core.Bootstrap start source";
	public static String target_agent_cmd = "deltad.core.Bootstrap start target";
	
	private AtomicBoolean running = new AtomicBoolean(true);
	
	private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
	
	public FailoverMonitor(String server) {
		if ("source".equalsIgnoreCase(server)) {
			server_cmd = source_agent_cmd;
		} else if ("target".equalsIgnoreCase(server)) {
			server_cmd = target_agent_cmd;
		} else {
			throw  new IllegalArgumentException("server should be either source or target");
		}	
		this.server = server.toLowerCase();	
	}
	
	public void start() {
		scheduler.scheduleWithFixedDelay(run, -1, 10, TimeUnit.SECONDS);
//		try {
//			while(running.get()) {
//				monitor();
//				Thread.sleep(10000);
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		System.out.println("failover monitor started for " + server);
	}
	
	public void stop() {
		running.set(false);
		scheduler.shutdown();
	}
	
	Runnable run = new Thread() {
		public void run() {
			try {
				if (running.get()) {
					monitor();
				}
			} catch (Exception e) {
				System.out.println("fail to failover server " + server);
				e.printStackTrace();
			}
		}
	};
	
	private void monitor() throws IOException {
		Map<Integer, LocalVirtualMachine>  map = LocalVirtualMachine.getAllVirtualMachines();
		boolean found = false;
		for (LocalVirtualMachine machine : map.values()) {			
			if (machine.displayName().equalsIgnoreCase(server_cmd)) {
				found = true;
				System.out.println(server_cmd + " is running OK...");
				return;
			}
				
		}
		
		if (!found) {
			String osname = System.getProperty("os.name").toLowerCase();
			String dib = osname.indexOf("windows") >= 0 ? "rdi.bat" : "./rdi.sh";
			ProcessBuilder pb = new ProcessBuilder(dib, "start", server);
			pb.redirectErrorStream(true);
			Process process = pb.start();
			InputStream in = process.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
	}

}
