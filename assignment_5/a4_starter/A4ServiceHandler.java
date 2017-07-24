import java.awt.event.ComponentAdapter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;



import io.atomix.copycat.client.CopycatClient;


public class A4ServiceHandler implements A4Service.Iface {
    private Map<Integer,CopycatClient> clientMap;

    private String ccHost;
    private int ccPort;

    static Logger log;

	private final ExecutorService service = Executors.newSingleThreadExecutor();

    public A4ServiceHandler(String ccHost, int ccPort) {
		this.ccHost = ccHost;
		this.ccPort = ccPort;
		clientMap = new ConcurrentHashMap<>();

		BasicConfigurator.configure();
		log = Logger.getLogger(A4ServiceHandler.class.getName());

		// start thread to commit CommandBuffer every 10ms
		service.execute(new BatchTicker(ccHost, ccPort));
    }

    CopycatClient getCopycatClient() {
		int tid = (int)Thread.currentThread().getId();
		CopycatClient client = clientMap.get(tid);
		if (client == null) {
			client = CopycatClientFactory.buildCopycatClient(ccHost, ccPort);
			clientMap.put(tid, client);
		}
		return client;
    }

    public long fetchAndIncrement(String key) throws org.apache.thrift.TException {

		CopycatClient client = getCopycatClient();

    	CommandBuffer.addIncrementCommand(key);
		Long copyCatVal = client.submit(new GetQuery(key)).join();
		Long delta = CommandBuffer.getDelta(key);
		Long ret = copyCatVal + delta;


		log.info("FAI called: " + key + ":" + ret + "-- delta: " + delta + " copyCatVal: " + copyCatVal);
		return ret;

		// improve this part
//		synchronized (this) {
//			CopycatClient client = getCopycatClient();
//			Long ret = client.submit(new FAICommand(key)).join();
//			return ret;
//		}
    }

    public long fetchAndDecrement(String key) throws org.apache.thrift.TException {

		CopycatClient client = getCopycatClient();
		CommandBuffer.addDecrementCommand(key);
		Long ret = client.submit(new GetQuery(key)).join() + CommandBuffer.getDelta(key);
		log.info("FAD called: " + key + ":" + ret);
		return ret;

//		// improve this part
//		synchronized (this) {
//			CopycatClient client = getCopycatClient();
//			Long ret = client.submit(new FADCommand(key)).join();
//			return ret;
//		}
    }

    public long get(String key) throws org.apache.thrift.TException {
		CopycatClient client = getCopycatClient();
		Long ret = client.submit(new GetQuery(key)).join();
		log.info("get called: " + key + ":" + ret);
		return ret;
    }
}
