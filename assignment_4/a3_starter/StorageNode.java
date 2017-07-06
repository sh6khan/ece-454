import java.io.*;
import java.util.*;
import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		BasicConfigurator.configure();


		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		CuratorFramework curClient =
			CuratorFrameworkFactory.builder()
			.connectString(args[2])
			.retryPolicy(new RetryNTimes(10, 1000))
			.connectionTimeoutMs(1000)
			.sessionTimeoutMs(10000)
			.build();

		curClient.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
			}
		});
        KeyValueHandler kvHandler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(kvHandler);
		TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				server.serve();
			}
		}).start();

		// create an ephemeral node in ZooKeeper
        String fullConnectionString = args[0] + ":" + String.valueOf(args[1]);
        curClient.create().withMode(CreateMode.EPHEMERAL).forPath("/gla/" + fullConnectionString, fullConnectionString.getBytes());

        // set up watcher on the children
        NodeWatcher nodeWatcher = new NodeWatcher(curClient);
        List<String> children = curClient.getChildren().usingWatcher(nodeWatcher).forPath("/gla");
        nodeWatcher.classifyNode(children.size());


    }
}
