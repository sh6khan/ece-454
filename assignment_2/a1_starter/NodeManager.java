import java.util.concurrent.ConcurrentHashMap;


public class NodeManager {

    // a concurentHash map that stores the key value representing the BENode available
    // key is hostname + port, Value is NodeInfo Object
    private static ConcurrentHashMap<String, NodeInfo> nodeList = new ConcurrentHashMap<>();


    public static NodeInfo getAvailableNodeInfo() {
        if (nodeList.size() == 0) {
            return null;
        }

        for (NodeInfo nodeInfo : nodeList.values()) {
            if (nodeInfo.isNotOccupied()) {
                return nodeInfo;
            }
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
        return nodeList.contains(nodeId);
    }
}
