import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import io.atomix.copycat.client.CopycatClient;


public class A4ServiceHandler implements A4Service.Iface {
    private Map<Integer,CopycatClient> clientMap;

    private String _ccHost;
    private int _ccPort;

	private final ExecutorService _service = Executors.newSingleThreadExecutor();
	private ReentrantReadWriteLock _lock;


    public A4ServiceHandler(String ccHost, int ccPort) {
        _ccHost = ccHost;
		_ccPort = ccPort;
		_lock = new ReentrantReadWriteLock();

		clientMap = new ConcurrentHashMap<>();

		// start thread to commit CommandBuffer every 10ms
		_service.execute(new BatchTicker(ccHost, ccPort, _lock));
    }

    CopycatClient getCopycatClient() {
		int tid = (int)Thread.currentThread().getId();
		CopycatClient client = clientMap.get(tid);
		if (client == null) {
			client = CopycatClientFactory.buildCopycatClient(_ccHost, _ccPort);
			clientMap.put(tid, client);
		}
		return client;
    }

    public long fetchAndIncrement(String key) throws org.apache.thrift.TException {
		_lock.readLock().lock();
        CopycatClient client = getCopycatClient();

		long retVal = CommandBuffer.getRetVal(key, client);
        CommandBuffer.addIncrementCommand(key);
		_lock.readLock().unlock();

        //System.out.println("FAI : " + key + " " + retVal);
        return retVal;


//		synchronized (this) {
//			CopycatClient client = getCopycatClient();
//			Long ret = client.submit(new FAICommand(key)).join();
//			return ret;
//		}
    }

    public long fetchAndDecrement(String key) throws org.apache.thrift.TException {
        _lock.readLock().lock();
        CopycatClient client = getCopycatClient();
        long retVal = CommandBuffer.getRetVal(key, client);
        CommandBuffer.addDecrementCommand(key);
        _lock.readLock().unlock();

        //System.out.println("FAD : " + key + " " + retVal);
        return retVal;


//		synchronized (this) {
//			CopycatClient client = getCopycatClient();
//			Long ret = client.submit(new FADCommand(key)).join();
//			return ret;
//		}
    }

    public long get(String key) throws org.apache.thrift.TException {
		CopycatClient client = getCopycatClient();
		CommandBuffer.commit(client, _lock);

		Long ret = client.submit(new GetQuery(key)).join();

		// System.out.println("GET called: " + key + " : " + ret);
		return ret;
    }
}
