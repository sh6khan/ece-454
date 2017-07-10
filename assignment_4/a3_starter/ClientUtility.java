import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class ClientUtility {

    public static final int BACKUP_POOL_NUM = 1;
    public static final int PRIMARY_POOL_NUM = 16;

    /**
     * A queue holding all this good shit
     */
    private static LinkedBlockingQueue<ThriftClient> clientObjectPool;

    static public void populateClientObjectPool(String host, Integer port, int cap) {
        // close all the existing tranpsorts
        if (clientObjectPool != null) {
            for (ThriftClient client : clientObjectPool) {
                client.closeTransport();
            }
        }

        // reset the clientObjectPool
        clientObjectPool = new LinkedBlockingQueue<ThriftClient>(cap);

        // populate the clientObjectPool
        for (int i = 0; i < cap; i++) {
            ThriftClient client = generateRPCClient(host, port);

            try {
                clientObjectPool.put(client);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // TODO: extract sleeping to outside the method call
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException e) {
//            System.out.println("Unable to sleep");
//        }
    }

    /**
     * Grab then next available client from the queue
     * @return
     */
    static public ThriftClient getAvailable() throws InterruptedException {
        return clientObjectPool.poll(50L, TimeUnit.SECONDS);
    }

    static public void makeAvailable(ThriftClient client) {
        try {
            clientObjectPool.put(client);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Generate Thrift RPC client to connect to a service
     * Pre opens the transport
     *
     * This function catches an errors thrown by the attempt to make a
     * connection to the remote service. When using this function you should
     * wrap it around in a retry
     *
     * @param host
     * @param port
     * @return An Opened transport client or null if failed
     */
    static private ThriftClient generateRPCClient(String host, Integer port) {
        try {
            TSocket sock = new TSocket(host, port);
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            return new ThriftClient(new KeyValueService.Client(protocol), transport);
        } catch (Exception e) {
            System.out.println("Unable to connect to primary");
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Extract sibling info for when PRIMARY needs to talk to BACKUP and for
     * when BACKUP needs to talk to PRIMARY
     *
     * @param children
     * @param zkNode
     * @param role
     * @param curClient
     * @return address representing host and port of sibling {@code StorageNode}
     * @throws Exception
     */
    static public InetSocketAddress extractSiblingInfo(
            List<String> children,
            String zkNode,
            KeyValueHandler.ROLE role,
            CuratorFramework curClient) throws Exception {

        String siblingZnode = determineSiblingZnode(children, role);
        byte[] data = curClient.getData().forPath(zkNode + "/" + siblingZnode);

        String strData = new String(data);
        String[] primary = strData.split(":");

        System.out.println(String.format("%s found connection info for sibling at %s:%s", role, primary[0], primary[1]));

        return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
    }

    /**
     * Determines the znode name for the other sibling under the list of
     * children
     *
     * A3Client determines primary by lexigraphically sorting the Znode names. The first
     * element in that sorted list will be the primary.
     *
     * @param children - a sorted list of all the children Znodes (should always be 2)
     */
    static private String determineSiblingZnode(List<String> children, KeyValueHandler.ROLE role) {
        if (children.size() != 2 ) {
            String msg = String.format("Wrong number of child nodes, expected 2 got :%d", children.size());
            throw new RuntimeException(msg);
        }

        // re-sort to make sure everything is working
        Collections.sort(children);

        // if current node is BACKUP we want to connect to
        // PRIMARY which is the first element of the sorted list
        if (role.equals(KeyValueHandler.ROLE.BACKUP)) {
            return children.get(0);
        }

        // if current node is PRIMARY we want to connec to
        // BACKUP which is the second element of the sorted list
        if (role.equals(KeyValueHandler.ROLE.PRIMARY)) {
            return children.get(1);
        }

        // throw runtime error (should never happen)
        throw new RuntimeException(String.format("role expected PRIMARY or BACKUP got: %s", role));
    }
}
