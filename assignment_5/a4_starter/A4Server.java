import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.storage.Storage;

public class A4Server {
    static Logger log;

    public static void main(String [] args) throws Exception {
	if (args.length != 1) {
	    System.err.println("Usage: java A4Server server_num");
	    System.exit(-1);
	}

	// initialize log4j
	BasicConfigurator.configure();
	log = Logger.getLogger(A4Server.class.getName());

	// determine port numbers for Thrift and Copycat
	BufferedReader br = new BufferedReader(new FileReader("a4.config"));
	String line;
	List<Address> members = new ArrayList<>();
	int i = 0;
	int ccServerNum = Integer.parseInt(args[0]);
	int rpcPort = 0;	
	while ((line = br.readLine()) != null) {
	    String[] parts = line.split(" ");
	    String nextHost = parts[0];
	    int nextCcPort = Integer.valueOf(parts[1]);
	    int nextRpcPort = Integer.valueOf(parts[2]);
	    members.add(new Address(nextHost, nextCcPort));
	    if (i == ccServerNum) {
		rpcPort = nextRpcPort;
	    }
	    i++;
	}

	// launch Copycat server
	Address myAddress = members.get(ccServerNum);
	log.info("Launching Copycat server number " + ccServerNum + " on " + myAddress.host() + "/" + myAddress.port());
	CopycatServer ccServer = CopycatServer.builder(myAddress)
	    .withStateMachine(A4StateMachine::new)
	    .withTransport(NettyTransport.builder().withThreads(8).build())
	    .withStorage(Storage.builder()
			 .withDirectory("copycat_data_" + ccServerNum)
			 .withMaxSegmentSize(1024 * 1024 * 32)
			 .withMinorCompactionInterval(Duration.ofMinutes(1))
			 .withMajorCompactionInterval(Duration.ofMinutes(15))
			 .build())
	    .build();

	ccServer.serializer().register(GetQuery.class, 1);
	ccServer.serializer().register(FAICommand.class, 2);
	ccServer.serializer().register(FADCommand.class, 3);
	ccServer.bootstrap(members).join();	

	// launch Thrift server
	log.info("Launching RPC server on port " + rpcPort);
	A4Service.Processor processor = new A4Service.Processor(new A4ServiceHandler(myAddress.host(), myAddress.port()));
	TServerSocket socket = new TServerSocket(rpcPort);
	TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	sargs.maxWorkerThreads(8);
	TThreadPoolServer rpcServer = new TThreadPoolServer(sargs);
	rpcServer.serve();
    }
}
