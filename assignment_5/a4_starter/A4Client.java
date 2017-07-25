import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import ca.uwaterloo.watca.ExecutionLogger;
import io.atomix.catalyst.transport.Address;

public class A4Client {
	static Logger log;

	int numThreads;
	int numSeconds;
	int keySpaceSize;
	volatile boolean done = false;
	AtomicInteger globalNumOps;
	AtomicInteger fai;
	AtomicInteger fad;
	ConcurrentHashMap<String, AtomicInteger> fadCount;
	ConcurrentHashMap<String, AtomicInteger> faiCount;
	volatile boolean getPrinted = false;
	List<Address> rpcAddresses;
	static ExecutionLogger exlog = new ExecutionLogger("execution.log");

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.err.println("Usage: java A4Client num_threads num_seconds key_space_size");
			System.exit(-1);
		}

		BasicConfigurator.configure();
		log = Logger.getLogger(A4Client.class.getName());
		exlog.start();
		A4Client client = new A4Client(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));

		try {
			client.execute();
		} catch (Exception e) {
			log.error("Uncaught exception", e);
		} finally {
		}
		exlog.stop();
	}

	A4Client(int numThreads, int numSeconds, int keySpaceSize) throws IOException {
		this.numThreads = numThreads;
		this.numSeconds = numSeconds;
		this.keySpaceSize = keySpaceSize;
		globalNumOps = new AtomicInteger();
		fai = new AtomicInteger(0);
		fad = new AtomicInteger(0);
		fadCount = new ConcurrentHashMap<String, AtomicInteger>();
		faiCount = new ConcurrentHashMap<String, AtomicInteger>();

		BufferedReader br = new BufferedReader(new FileReader("a4.config"));
		String line;
		rpcAddresses = new ArrayList<>();
		while ((line = br.readLine()) != null) {
			String[] parts = line.split(" ");
			String nextHost = parts[0];
			int nextRpcPort = Integer.valueOf(parts[2]);
			rpcAddresses.add(new Address(nextHost, nextRpcPort));
		}
		br.close();
	}

	void execute() throws Exception {
		List<Thread> tlist = new ArrayList<>();
		List<MyRunnable> rlist = new ArrayList<>();
		for (int i = 0; i < numThreads; i++) {
			MyRunnable r = new MyRunnable();
			Thread t = new Thread(r);
			tlist.add(t);
			rlist.add(r);
		}
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numThreads; i++) {
			tlist.get(i).start();
		}
		log.info("Done starting " + numThreads + " threads...");
		Thread.sleep(numSeconds * 1000);
		done = true;

		for (Thread t : tlist) {
			t.join();
		}

		long estimatedTime = System.currentTimeMillis() - startTime;
		int tput = (int) (1000f * globalNumOps.get() / estimatedTime);
		log.info("Aggregate throughput: " + tput + " RPCs/s");
		long totalLatency = 0;
		for (MyRunnable r : rlist) {
			totalLatency += r.getTotalTime();
		}
		double avgLatency = (double) totalLatency / globalNumOps.get() / 1000;
		log.info("Average latency: " + ((int) (avgLatency * 100)) / 100f + " ms");

		HashSet<String> keySet = new HashSet<String>();
		keySet.addAll(faiCount.keySet());
		keySet.addAll(fadCount.keySet());

		A4Service.Client client = getThriftClient();

		for (String key : keySet) {
			StringBuilder sb = new StringBuilder();
			sb.append(key + " FAI: ");
			int fai = faiCount.getOrDefault(key, new AtomicInteger(0)).get();
			sb.append(fai);
			sb.append(" FAD: ");
			int fad = fadCount.getOrDefault(key, new AtomicInteger(0)).get();
			sb.append(fad);
			sb.append(" GET: ");
			Long ret = client.get(key);
			sb.append(ret);
			if (fai - fad == ret) {
			} else {
				sb.append(" FAIL");
			}
			System.out.println(sb.toString());
		}
	}

	A4Service.Client getThriftClient() {
		while (true) {
			int tid = (int) Thread.currentThread().getId();
			Address address = rpcAddresses.get(tid % rpcAddresses.size());
			log.info("Thread " + tid + " connecting to " + address);
			try {
				TSocket sock = new TSocket(address.host(), address.port());
				TTransport transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				return new A4Service.Client(protocol);
			} catch (Exception e) {
				log.error("Unable to connect to " + address);
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	class MyRunnable implements Runnable {
		long totalTime;
		A4Service.Client client;

		MyRunnable() throws TException {
		}

		long getTotalTime() {
			return totalTime;
		}

		public void run() {
			client = getThriftClient();
			Random rand = new Random();
			totalTime = 0;
			int numOps = 0;
			long tid = Thread.currentThread().getId();
			try {
				while (!done) {
					long startTime = System.nanoTime();
					while (true) {
						if (rand.nextBoolean()) {
							try {
								String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
								Long resp = client.fetchAndIncrement(key);
								exlog.logWriteInvocation(tid, "FAI-" + key, String.valueOf(resp));
								faiCount.putIfAbsent(key, new AtomicInteger(0));
								faiCount.get(key).getAndIncrement();
								fai.getAndIncrement();
								numOps++;
								break;
							} catch (Exception e) {
								log.error("Exception during fetchAndIncrement", e);
								client = getThriftClient();
							}
						} else {
//							try {
//								String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);
//								Long resp = client.fetchAndDecrement(key);
//								exlog.logWriteInvocation(tid, "FAD-" + key, String.valueOf(resp));
//								fadCount.putIfAbsent(key, new AtomicInteger(0));
//								fadCount.get(key).getAndIncrement();
//								fad.getAndIncrement();
//								numOps++;
//								break;
//							} catch (Exception e) {
//								log.error("Exception during fetchAndDecrement", e);
//								client = getThriftClient();
//							}
						}
					}
					long diffTime = System.nanoTime() - startTime;
					totalTime += diffTime / 1000;
				}
				/*
				 * if (!getPrinted) { getPrinted = true; String key = "key-" +
				 * (Math.abs(rand.nextLong()) % keySpaceSize); Long resp =
				 * client.get(key); exlog.logWriteInvocation(tid, "GET-" + key,
				 * String.valueOf(resp)); exlog.logWriteInvocation(tid,
				 * "FAICount", String.valueOf(faiCount.get(key).get()));
				 * exlog.logWriteInvocation(tid, "FADCount",
				 * String.valueOf(fadCount.get(key).get()));
				 * exlog.logWriteInvocation(tid, "FAICount",
				 * String.valueOf(fai.get())); exlog.logWriteInvocation(tid,
				 * "FADCount", String.valueOf(fad.get())); }
				 */
			} catch (Exception x) {
				x.printStackTrace();
			}
			globalNumOps.addAndGet(numOps);
		}
	}
}
