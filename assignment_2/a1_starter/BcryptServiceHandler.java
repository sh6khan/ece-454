import java.lang.reflect.Array;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.mindrot.jbcrypt.BCrypt;

import javax.xml.soap.Node;

public class BcryptServiceHandler implements BcryptService.Iface {

    private boolean _isBENode;
    private final ExecutorService service = Executors.newFixedThreadPool(4);

    public BcryptServiceHandler(boolean isBENode){
        _isBENode = isBENode;
    }

    public List<String> hashPassword(List<String> passwords, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {
        if (passwords.size() == 0) {
            throw new IllegalArgument("Cannot have empty password list");
        }

        if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("logRounds need to be between 4 and 30 ");
        }


        TTransport transport = null;
        String[] res = new String[passwords.size()];

        // If BENode, then compute the hash right here
        if (_isBENode) {
            BatchTracker.receivedBatch();
            try {
                int size = passwords.size();
                int numThreads = Math.min(size, 4);
                int chunkSize = size / numThreads;
                CountDownLatch latch = new CountDownLatch(numThreads);
                if (size > 1) {
                    for (int i = 0; i < numThreads; i++) {
                        int startInd = i * chunkSize;
                        int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
                        service.execute(new MultiThreadHash(passwords, logRounds, res, startInd, endInd, latch));
                    }
                    latch.await();
                } else {
                    System.out.println("using single thread for hashing");
                    hashPasswordImpl(passwords, logRounds, res, 0, passwords.size());
                }

                // we update the timer of the receivedBatch because we don't
                // want the time it took to process the batch as a part of the
                // timeout
                BatchTracker.receivedBatch();
                List<String> ret = Arrays.asList(res);
                System.out.println("sizes: " + passwords.size() + " " + ret.size());
                return ret;
            } catch (Exception e) {
                throw new IllegalArgument(e.getMessage());
            }

        } else {
            Instant startTime = Instant.now();
            NodeInfo nodeInfo = NodeManager.getAvailableNodeInfo();
            System.out.println("Find available node took: " + Duration.between(Instant.now(), startTime).toNanos());

            while (nodeInfo != null) {

                // This is an FENode, try offloading to the BENode
                BcryptService.Client client = nodeInfo.getClient();
                transport = nodeInfo.getTransport();

                // if the client and transport of a BE node is available then have FE offload the work to the
                // BE Node
                System.out.println("moving work over to the back end node: " + nodeInfo.nodeId);
                try {
                    if (!transport.isOpen()){
                        transport.open();
                    }
                    nodeInfo.addLoad(passwords.size(), logRounds);
                    List<String> BEResult = client.hashPassword(passwords, logRounds);
                    nodeInfo.reduceLoad(passwords.size(), logRounds);
                    nodeInfo.markAvailable();

                    return BEResult;
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println(e.getMessage());
                    // if BENode threw an exception, then we simply remove it from NodeManager
                    NodeManager.removeNode(nodeInfo.nodeId);

                    System.out.println("BENode at " + nodeInfo.nodeId + " is dead :( Removing from NodeManager");

                    nodeInfo = NodeManager.getAvailableNodeInfo();
                } finally {
                    if (transport != null && transport.isOpen()) {
                        transport.close();
                    }
                }
            }

            // We tried to offload  the work to each available BENode, but they all failed
            // therefore, have the FENode do the work
            System.out.println("All BENodes are dead");
            try {
                hashPasswordImpl(passwords, logRounds, res, 0, passwords.size());
                return Arrays.asList(res);
            } catch (Exception ex) {
                throw new IllegalArgument(ex.getMessage());
            }
        }
    }

    public List<Boolean> checkPassword(List<String> passwords, List<String> hashes) throws IllegalArgument, org.apache.thrift.TException
    {
        TTransport transport = null;
        Boolean[] res = new Boolean[passwords.size()];

        // If BENode, then compute the hash right here
        if (_isBENode) {
            BatchTracker.receivedBatch();
            try {

                // throw error if the size of hashes
                if (passwords.size() != hashes.size()) {
                    throw new Exception("passwords and hashes are not equal.");
                }

                if (passwords.size() == 0) {
                    throw new Exception(("password list cannot be empty"));
                }


                int size = passwords.size();
                int numThreads = Math.min(size, 4);
                int chunkSize = size / numThreads;
                CountDownLatch latch = new CountDownLatch(numThreads);
                if (size > 1) {
                    for (int i = 0; i < numThreads; i++) {
                        int startInd = i * chunkSize;
                        int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
                        service.execute(new MultiThreadCheck(passwords, hashes, res, startInd, endInd, latch));
                    }
                    latch.await();
                } else {
                    System.out.println("using single thread for checking");
                    checkPasswordImpl(passwords, hashes, res, 0, passwords.size());
                }
                return Arrays.asList(res);
            } catch (Exception e) {
                throw new IllegalArgument(e.getMessage());
            }

        } else {
            NodeInfo nodeInfo = NodeManager.getAvailableNodeInfo();

            while (nodeInfo != null) {

                // This is an FENode, try offloading to the BENode
                BcryptService.Client client = nodeInfo.getClient();
                transport = nodeInfo.getTransport();

                // if the client and transport of a BE node is available then have FE offload the work to the
                // BE Node
                System.out.println("moving work over to the back end node: " + nodeInfo.nodeId);
                try {
                    if (!transport.isOpen()){
                        transport.open();
                    }
                    nodeInfo.addLoad(passwords.size(), (short)0);
                    List<Boolean> BEResult = client.checkPassword(passwords, hashes);
                    nodeInfo.reduceLoad(passwords.size(), (short)0);
                    nodeInfo.markAvailable();

                    return BEResult;
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    // if BENode threw an exception, then we simply remove it from NodeManager
                    NodeManager.removeNode(nodeInfo.nodeId);

                    System.out.println("BENode at " + nodeInfo.nodeId + " is dead :( Removing from NodeManager");

                    nodeInfo = NodeManager.getAvailableNodeInfo();
                } finally {
                    if (transport != null && transport.isOpen()) {
                        transport.close();
                    }
                }
            }

            // We tried to offload  the work to each available BENode, but they all failed
            // therefore, have the FENode do the work
            System.out.println("All BENodes are dead");
            try {
                checkPasswordImpl(passwords, hashes, res, 0, passwords.size());
                return Arrays.asList(res);
            } catch (Exception ex) {
                throw new IllegalArgument(ex.getMessage());
            }
        }
    }

    private void checkPasswordImpl(List<String> passwords, List<String> hashes, Boolean[] res, int start, int end) {
        System.out.println("Checking Passwords of size: " + (end - start));

        String password;
        String hash;
        for (int i = start; i < end; i++) {
            password = passwords.get(i);
            hash = hashes.get(i);
            try{
                res[i] = (BCrypt.checkpw(password, hash));
            } catch (Exception e){
                res[i] = false;
            }

        }
    }
    
    public Map<String, String> heartBeat(String hostname, String port) throws IllegalArgument, org.apache.thrift.TException {
        System.out.println("received heart beat from: " + hostname + port);

      try {
          String nodeId = hostname + port;
          if (!NodeManager.containsNode(nodeId)) {
              NodeInfo nodeInfo = new NodeInfo(hostname, port);
              NodeManager.addNode(nodeId, nodeInfo);
          }

          HashMap<String, String> map = new HashMap<>();
          map.put(hostname, port);
          return map;

      } catch (Exception e) {
          e.printStackTrace();
          throw new IllegalArgument(e.getMessage());
      }
    }

    private void hashPasswordImpl(List<String> passwords, short logRounds, String[] res, int start, int end ) {
        System.out.println("Hashing Passwords of size: " + (end - start));

        for (int i = start; i < end; i++) {
            res[i] = BCrypt.hashpw(passwords.get(i), BCrypt.gensalt(logRounds));
        }
    }

    class MultiThreadHash implements Runnable {
        private List<String> _passwords;
        private short _logRounds;
        private String[] _res;
        private int _start;
        private int _end;
        private CountDownLatch _latch;

        public MultiThreadHash(List<String> passwords, short logRounds, String[] res, int start, int end, CountDownLatch latch) {
            _logRounds = logRounds;
            _passwords = passwords;
            _res = res;
            _start = start;
            _end = end;
            _latch = latch;
        }

        @Override
        public void run() {
            hashPasswordImpl(_passwords, _logRounds, _res, _start, _end);
            _latch.countDown();
        }
    }

    class MultiThreadCheck implements Runnable {
        private List<String> _passwords;
        private List<String> _hashes;
        private Boolean[] _res;
        int _start;
        int _end;
        CountDownLatch _latch;


        public MultiThreadCheck(List<String> passwords, List<String> hashes, Boolean[] res, int start, int end, CountDownLatch latch) {
            _passwords = passwords;
            _hashes = hashes;
            _res = res;
            _start = start;
            _end = end;
            _latch = latch;
        }

        @Override
        public void run() {
            checkPasswordImpl(_passwords, _hashes, _res, _start, _end);
            _latch.countDown();
        }
    }
}
