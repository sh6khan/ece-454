import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.atomix.catalyst.transport.Address;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class A4Client {
    static Logger log;
    
    int numThreads;
    int numSeconds;
    int keySpaceSize;
    volatile boolean done = false;
    AtomicInteger globalNumOps;
    AtomicInteger globalErrCount;
    List<Address> rpcAddresses;

	Map<String, Long> globalCache = new ConcurrentHashMap<>();

    public static void main(String [] args) throws IOException {
	if (args.length != 3) {
	    System.err.println("Usage: java A4Client num_threads num_seconds key_space_size");
	    System.exit(-1);
	}

	BasicConfigurator.configure();
	log = Logger.getLogger(A4Client.class.getName());

	A4Client client = new A4Client(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
	
	try {
	    client.execute();
	} catch (Exception e) {
	    log.error("Uncaught exception", e);
	} finally {
	}
    }

    A4Client(int numThreads, int numSeconds, int keySpaceSize) throws IOException {
	this.numThreads = numThreads;
	this.numSeconds = numSeconds;
	this.keySpaceSize = keySpaceSize;
	globalNumOps = new AtomicInteger();
	globalErrCount = new AtomicInteger();

	BufferedReader br = new BufferedReader(new FileReader("a4.config"));
	String line;
	rpcAddresses = new ArrayList<>();
	while ((line = br.readLine()) != null) {
	    String[] parts = line.split(" ");
	    String nextHost = parts[0];
	    int nextRpcPort = Integer.valueOf(parts[2]);
	    rpcAddresses.add(new Address(nextHost, nextRpcPort));
	}
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
		for (Thread t: tlist) {
			t.join();
		}
		long estimatedTime = System.currentTimeMillis() - startTime;
		int tput = (int)(1000f * globalNumOps.get() / estimatedTime);
		log.info("Aggregate throughput: " + tput + " RPCs/s");
		long totalLatency = 0;
		for (MyRunnable r: rlist) {
			totalLatency += r.getTotalTime();
		}
		double avgLatency = (double)totalLatency / globalNumOps.get() / 1000;
		log.info("Average latency: " + ((int)(avgLatency*100))/100f + " ms");

		log.info("Err Count: " + globalErrCount.get());


		// this is the end
		A4Service.Client client = getThriftClient();
		for (Map.Entry<String, Long> entry : globalCache.entrySet()) {
			long actual = client.get(entry.getKey());
			if (actual != entry.getValue()) {
				System.out.println("FAILED FAD on key: " + entry.getKey() + " actual: " + actual + " expected: " + entry.getValue() );
			}
		}
    }

    A4Service.Client getThriftClient() {
	while (true) {
	    int tid = (int)Thread.currentThread().getId();
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
	    } catch (InterruptedException e) {}
	}
    }

    class MyRunnable implements Runnable {
	long totalTime;
	A4Service.Client client;
	MyRunnable() throws TException {
	}

	long getTotalTime() { return totalTime; }

	public void run() {
	    client = getThriftClient();
	    Random rand = new Random();
	    totalTime = 0;
	    long tid = Thread.currentThread().getId();
	    int numOps = 0;
	    int errCount = 0;





	    try {
			while (!done) {
				long startTime = System.nanoTime();

				while (true) {
					int method = rand.nextInt(3);
					String key = "key-" + (Math.abs(rand.nextLong()) % keySpaceSize);

					if (method == 0) {
						try {
							long expected = globalCache.getOrDefault(key, 0L);
							globalCache.put(key, expected + 1);

							long retVal = client.fetchAndIncrement(key);

							if (expected != retVal) {
								System.out.println("FAILED FAI on key: " + key + " actual: " + retVal + " expected: " + expected );
								errCount += 1;
							}

							numOps++;
							break;

						} catch (Exception e) {
							log.error("Exception during fetchAndIncrement", e);
							client = getThriftClient();
						}
					}

					if (method == 1) {
						try {
							long expected = globalCache.get(key);
							long retVal = client.get(key);

							if (expected != retVal) {
								System.out.println("FAILED GET on key: " + key + " actual: " + retVal + " expected: " + expected );
								errCount += 1;
							}

							numOps++;
							break;
						} catch (Exception e) {
							log.error("Exception during get", e);
							client = getThriftClient();
						}
					}

					try {
						long expected = globalCache.getOrDefault(key, 0L);
						globalCache.put(key, expected - 1);

						long retVal = client.fetchAndDecrement(key);

						if (expected != retVal) {
							System.out.println("FAILED FAD on key: " + key + " actual: " + retVal + " expected: " + expected );
							errCount += 1;
						}


						numOps++;
						break;
					} catch (Exception e) {
						log.error("Exception during get", e);
						client = getThriftClient();
					}
				}
				long diffTime = System.nanoTime() - startTime;
				totalTime += diffTime / 1000;
			}
	    } catch (Exception x) {
			x.printStackTrace();
	    } 	
	    globalNumOps.addAndGet(numOps);
	    globalErrCount.addAndGet(errCount);
	}
    }
}
