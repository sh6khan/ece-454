import java.util.ArrayList;
import java.util.HashSet;

import org.apache.thrift.transport.TTransport;

public class NodeManager {

    // a list of indexes to available BENodes
    private static ArrayList<Boolean> availableNodes = new ArrayList<>();

    // used by FENode to track all existing BENodes
    private static ArrayList<BcryptService.Client> BENodeList = new ArrayList<>();

    // used by FENode to track all existing BENodes
    private static ArrayList<TTransport> transportList = new ArrayList<>();

    // used by FENode to avoid re-adding the same BENode on the periodic heartbeat
    private static ArrayList<String> registeredNodes = new ArrayList<>();


    public static void registerNode(String nodeId) {
        registeredNodes.add(nodeId);
    }

    public static boolean isRegistered(String nodeId) {
        return registeredNodes.contains(nodeId);
    }

    public static Integer getAvailableNodeIndex() {
        if (availableNodes.size() == 0) {
            return null;
        }

        for (int i = 0; i < availableNodes.size(); i++) {
            if (availableNodes.get(i) == Boolean.TRUE) {
                return i;
            }
        }

        return null;
    }

    public static BcryptService.Client getNodeClient(int index) {
        return BENodeList.get(index);
    }

    public static TTransport getNodeTransport(int index) {
        return transportList.get(index);
    }

    public static void markUnavailable (int index) {
        availableNodes.set(index, Boolean.FALSE);
    }

    public static void markAvailable (int index) {
        availableNodes.set(index, Boolean.TRUE);
    }


    /**
     * Add a new BENode client that is available to the FENodes
     */
    public static void addNode(BcryptService.Client client, TTransport transport) {
        BENodeList.add(client);
        transportList.add(transport);

        availableNodes.add(Boolean.TRUE);
        System.out.println("available nodes size: " + availableNodes.size());
    }

    /**
     * When there was an error from one of the BENodes, either from throwing an
     * Exception or from BENodes being shut down, remove it from the NodeManager
     *
     * @param index - the index in each array where the node info is stored
     */
    public static void removeNode(int index) {
        availableNodes.remove(index);
        BENodeList.remove(index);
        transportList.remove(index);
        registeredNodes.remove(index);
    }
}
