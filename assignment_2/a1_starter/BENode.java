import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class BENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());

		String hostFE = "localhost";
		int portFE = 10000;
		int portBE = 11000;
		log.info("Launching BE node on port " + portBE);

		// alerting FENode that BENode exists
		TSocket sock = new TSocket("localhost", 10000);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client client = new BcryptService.Client(protocol);

		transport.open();
		Map<String, String> res = client.heartBeat("localhost", "11000");
		transport.close();

		// launch Thrift server
		BcryptService.Processor processor = new BcryptService.Processor(new BcryptServiceHandler());
		TServerSocket socket = new TServerSocket(portBE);
		TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		TSimpleServer server = new TSimpleServer(sargs);
		server.serve();
    }
}
