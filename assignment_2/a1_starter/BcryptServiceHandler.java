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
        TTransport transport = null;
        List<String> res = new ArrayList<>();

        // If BENode, then compute the hash right here
        if (_isBENode) {
            BatchTracker.receivedBatch();
            try {
                int size = passwords.size();
                int numThreads = Math.min(size, 4);
                int chunkSize = size / numThreads;

                if (size > 1) {
                    List<Future<List<String>>> futures = new ArrayList<>();
                    for (int i = 0; i < numThreads; i++) {
                        int startInd = i * chunkSize;
                        int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
                        MultiThreadHash  myCallable = new MultiThreadHash(passwords.subList(startInd, endInd), logRounds);
                        futures.add(service.submit(myCallable));
                    }
                    for (Future<List<String>> f: futures) {
                        res.addAll(f.get());
                    }
                } else {
                    System.out.println("using single thread for hashing");
                    return hashPasswordImpl(passwords, logRounds);
                }


                // we update the timer of the receivedBatch because we don't
                // want the time it took to process the batch as a part of the
                // timeout
                BatchTracker.receivedBatch();
                System.out.println("sizes: " + passwords.size() + " " + res.size());
                return res;
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
                    transport.open();
                    nodeInfo.markOccupied();
                    nodeInfo.addLoad(passwords.size(), logRounds);
                    List<String> BEResult = client.hashPassword(passwords, logRounds);
                    nodeInfo.reduceLoad(passwords.size(), logRounds);
                    nodeInfo.markAvailable();

                    return BEResult;
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    // if BENode threw an exception, then we simply remove it from NodeManager
                    NodeManager.removeNode(nodeInfo.nodeId);

                    System.out.println("BENode at " + nodeInfo.nodeId + " is dead :( Removing from NodeManager");

                    nodeInfo = NodeManager.getAvailableNodeInfo();
                } finally {
                    if (transport != null) {
                        transport.close();
                    }
                }
            }

            // We tried to offload  the work to each available BENode, but they all failed
            // therefore, have the FENode do the work
            System.out.println("All BENodes are dead");
            try {
                return hashPasswordImpl(passwords, logRounds);
            } catch (Exception ex) {
                throw new IllegalArgument(ex.getMessage());
            }
        }
    }

    public List<Boolean> checkPassword(List<String> passwords, List<String> hashes) throws IllegalArgument, org.apache.thrift.TException
    {
        TTransport transport = null;
        List<Boolean> res = new ArrayList<>();

        // If BENode, then compute the hash right here
        if (_isBENode) {
            BatchTracker.receivedBatch();
            try {

                // throw error if the size of hashes
                if (passwords.size() != hashes.size()) {
                    throw new Exception("passwords and hashes are not equal.");
                }

                int size = passwords.size();
                int numThreads = Math.min(size, 4);
                int chunkSize = size / numThreads;

                if (size > 1) {
                    List<Future<List<Boolean>>> futures = new ArrayList<>();
                    for (int i = 0; i < numThreads; i++) {
                        int startInd = i * chunkSize;
                        int endInd = i == numThreads - 1 ? size : (i + 1) * chunkSize;
                        MultiThreadCheck  myCallable = new MultiThreadCheck(
                                passwords.subList(startInd, endInd),
                                hashes.subList(startInd, endInd));
                        futures.add(service.submit(myCallable));
                    }
                    for (Future<List<Boolean>> f: futures) {
                        res.addAll(f.get());
                    }
                } else {
                    System.out.println("using single thread for checking");
                    return checkPasswordImpl(passwords, hashes);
                }
                return res;
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
                    transport.open();
                    nodeInfo.markOccupied();
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
                    if (transport != null) {
                        transport.close();
                    }
                }
            }

            // We tried to offload  the work to each available BENode, but they all failed
            // therefore, have the FENode do the work
            System.out.println("All BENodes are dead");
            try {
                return checkPasswordImpl(passwords, hashes);
            } catch (Exception ex) {
                throw new IllegalArgument(ex.getMessage());
            }
        }
    }

    private List<Boolean> checkPasswordImpl(List<String> passwords, List<String> hashes) throws Exception {
        System.out.println("Checking Passwords of size: " + passwords.size());

        List<Boolean> ret = new ArrayList<>();

        String password;
        String hash;
        for (int i = 0; i < passwords.size(); i++) {
            password = passwords.get(i);
            hash = hashes.get(i);
            ret.add(BCrypt.checkpw(password, hash));
        }
        return ret;
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

    private List<String> hashPasswordImpl(List<String> passwords, short logRounds) throws Exception {
        System.out.println("Hashing Passwords of size: " + passwords.size());

        List<String> ret = new ArrayList<>();
        String hashedPassword;

        for (String password : passwords) {
            hashedPassword = BCrypt.hashpw(password, BCrypt.gensalt(logRounds));
            ret.add(hashedPassword);
        }

        return ret;
    }

    class MultiThreadHash implements Callable<List<String>> {
        private List<String> _passwords;
        short _logRounds;

        public MultiThreadHash(List<String> passwords, short logRounds) {
            _passwords = passwords;
            _logRounds = logRounds;
        }

        @Override
        public List<String> call() throws Exception{
            return hashPasswordImpl(_passwords, _logRounds);
        }
    }

    class MultiThreadCheck implements Callable<List<Boolean>> {
        private List<String> _passwords;
        private List<String> _hashes;

        public MultiThreadCheck(List<String> passwords, List<String> hashes) {
            _passwords = passwords;
            _hashes = hashes;
        }

        @Override
        public List<Boolean> call() throws Exception{
            return checkPasswordImpl(_passwords, _hashes);
        }
    }
}
