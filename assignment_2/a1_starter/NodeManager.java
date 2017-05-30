import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;


public class NodeManager {

    // a concurentHash map that stores the key value representing the BENode available
    // key is hostname + port, Value is NodeInfo Object
    private static ConcurrentHashMap<String, NodeInfo> nodeList = new ConcurrentHashMap<>();


    public static synchronized NodeInfo getAvailableNodeInfo() {
        if (nodeList.size() == 0) {
            return null;
        }

        // Try and see if a BENode is available
        for (NodeInfo node : nodeList.values()) {
            if (node.isNotOccupied()) {
                node.markOccupied();
                return node;
            }
        }

        NodeInfo nodeInfo = nodeList.values().iterator().next();
        for (NodeInfo node : nodeList.values()) {
            if (nodeInfo.getLoad() > node.getLoad()) {
                nodeInfo = node;
            }
        }

        System.out.println("found min with load " + nodeInfo.getLoad());

        // somehow a node was deleted in between the last check and now
        if (nodeInfo == null) {
            return null;
        }

        // create new client
        String hostname = nodeInfo.getHostname();
        String port = nodeInfo.getPort();

        try {
            nodeInfo = new NodeInfo(hostname, port);
            return nodeInfo;
        } catch (Exception ex) {
            System.out.println("failed to create new Node from " + hostname + " " + port);
            // do nothing and return null;
        }

        return null;
    }

    /**
     * Add a new BENode client that is available to the FENodes
     */
    public static void addNode(String nodeId, NodeInfo nodeInfo) {
        nodeList.put(nodeId, nodeInfo);
        System.out.println("available nodes size: " + nodeList.size());
    }

    /**
     * When there was an error from one of the BENodes, either from throwing an
     * Exception or from BENodes being shut down, remove it from the NodeManager
     */
    public static void removeNode(String nodeId) {
       NodeInfo nodeInfo = nodeList.remove(nodeId);

       if (nodeInfo == null) {
           System.out.println("Tried to remove " + nodeId + "from nodeList but it did not exist");
       }
    }

    public static boolean containsNode(String nodeId) {
        return nodeList.containsKey(nodeId);
    }
}
