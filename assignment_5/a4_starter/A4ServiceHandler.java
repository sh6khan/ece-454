import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import io.atomix.copycat.client.CopycatClient;


public class A4ServiceHandler implements A4Service.Iface {
    private Map<Integer,CopycatClient> clientMap;

    private String ccHost;
    private int ccPort;

	private final ExecutorService service = Executors.newSingleThreadExecutor();


    public A4ServiceHandler(String ccHost, int ccPort) {
		this.ccHost = ccHost;
		this.ccPort = ccPort;
		clientMap = new ConcurrentHashMap<>();

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
		synchronized (this) {
			CopycatClient client = getCopycatClient();

			// CommandBuffer.commitIfNeeded(client);
			long copyCatVal = client.submit(new GetQuery(key)).join();

			long delta = CommandBuffer.addIncrementCommand(key);
			long ret = delta + copyCatVal;

			// System.out.println("FAI called: " + key + " : " + ret + " -- delta: " + delta + " copyCatVal: " + copyCatVal);
			return ret;

		}


//		synchronized (this) {
//			CopycatClient client = getCopycatClient();
//			Long ret = client.submit(new FAICommand(key)).join();
//			return ret;
//		}
    }

    public long fetchAndDecrement(String key) throws org.apache.thrift.TException {
		synchronized (this) {
			CopycatClient client = getCopycatClient();

			// CommandBuffer.commitIfNeeded(client);
			long copyCatVal = client.submit(new GetQuery(key)).join();

			long delta = CommandBuffer.addDecrementCommand(key);
			long ret = delta + copyCatVal;

			// System.out.println("FAD called: " + key + " : " + ret + " -- delta: " + delta + " copyCatVal: " + copyCatVal);
			return ret;
		}



//		synchronized (this) {
//			CopycatClient client = getCopycatClient();
//			Long ret = client.submit(new FADCommand(key)).join();
//			return ret;
//		}
    }

    public long get(String key) throws org.apache.thrift.TException {
		synchronized (this) {
			CopycatClient client = getCopycatClient();
			CommandBuffer.commit(client);

			Long ret = client.submit(new GetQuery(key)).join();
			// System.out.println("GET called: " + key + " : " + ret);
			return ret;
		}

    }
}
