import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.ServerSelectionStrategies;

public class A4ServiceHandler implements A4Service.Iface {
    private Map<Integer,CopycatClient> clientMap;
    private String ccHost;
    private int ccPort;

    public A4ServiceHandler(String ccHost, int ccPort) {
	this.ccHost = ccHost;
	this.ccPort = ccPort;
	clientMap = new ConcurrentHashMap<>();
    }

    CopycatClient getCopycatClient() {
	int tid = (int)Thread.currentThread().getId();
	CopycatClient client = clientMap.get(tid);
	if (client == null) {
	    List<Address> members = new ArrayList<>();
	    members.add(new Address(ccHost, ccPort));   
	    client = CopycatClient.builder()
		.withTransport(new NettyTransport())
		.build();    
	    client.serializer().register(GetQuery.class, 1);
	    client.serializer().register(FAICommand.class, 2);
	    client.connect(members).join();    
	    clientMap.put(tid, client);
	}
	return client;
    }

    public long fetchAndIncrement(String key) throws org.apache.thrift.TException
    {
	// improve this part
	synchronized (this) {
	    CopycatClient client = getCopycatClient();
	    Long ret = client.submit(new FAICommand()).join();
	    return ret;
	}
    }

    public long fetchAndDecrement(String key) throws org.apache.thrift.TException
    {
	// complete this part
	return 0;
    }

    public long get(String key) throws org.apache.thrift.TException
    {
	// complete this part
	return 0;
    }
}
