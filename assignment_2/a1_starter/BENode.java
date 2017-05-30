import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BENode {
    static Logger log;

    private static ExecutorService _executorService = Executors.newCachedThreadPool(new ThreadFactory() {
        private AtomicInteger threadCounter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            t.setName("worker-thread-" + threadCounter.incrementAndGet());
            return t;
        }
    });

    public static void main(String [] args) throws Exception {
		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());

		String hostFE = args[0];
		String hostBE = InetAddress.getLocalHost().getHostName();
		int portFE = Integer.parseInt(args[1]);
		int portBE = Integer.parseInt(args[2]);
		log.info("Launching BE node on port " + portBE);

		// Create a client to the FENode
		TSocket sock = new TSocket(hostFE, portFE);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);

		// Instantiate a BatchTracker instance to help determine if the FENode is still alive
        FENodeChecker FENodeConnection = new FENodeChecker(transport, client, hostBE, args[2]);

        // establish connection to instantly
        FENodeConnection.establishConnectionToFENode();

        // start thread to continuously check if the
        _executorService.submit(FENodeConnection);

		// launch Thrift server
		BcryptService.Processor processor = new BcryptService.Processor(new BcryptServiceHandler(true));
		TServerSocket socket = new TServerSocket(portBE);
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        sargs.transportFactory(new TFramedTransport.Factory());
        sargs.processorFactory(new TProcessorFactory(processor));
        TThreadPoolServer server = new TThreadPoolServer(sargs);
        server.serve();
    }
}
