import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.WatchedEvent;

import javax.xml.crypto.dsig.keyinfo.KeyValue;

public class NodeWatcher implements CuratorWatcher {
    private KeyValueHandler _kvHandler;
    private CuratorFramework _curClient;
    private KeyValueService.Client _siblingClient;
    private String _connectionString;

    public NodeWatcher(CuratorFramework curClient) {
        _curClient = curClient;
    }

    /**
     * based on the number of children currently registered in Zookeeper
     * we can change the role of the current node to be either BACKUP
     * or PRIMARY.
     *
     * If there is only one child then that means the current node is PRIMARY
     * If there is 2 children then that means someone else is PRIMARY therefore
     * the current node is BACKUP
     *
     * @param size - number of child znode names under our $USER znode
     */
    public void classifyNode(int size) {
        if (_kvHandler.getRole().equals(KeyValueHandler.ROLE.PRIMARY)) {
            return;
        }

        if (size == 1) {
            _kvHandler.setRole(KeyValueHandler.ROLE.PRIMARY);
        } else if (size == 2) {
            _kvHandler.setRole(KeyValueHandler.ROLE.BACKUP);
        } else {
            String msg = String.format("There are %d childNodes which makes 0 sense", size);
            throw new RuntimeException(msg);
        }

        System.out.println(_kvHandler.getRole());
    }


    private String getSiblingNode(String myName, List<String> children) {
        for (String child: children){
            if (!child.equals(myName)) {
                return child;
            }
        }
        throw new RuntimeException();
    }
    /**
     * Callback function on the watcher
     *
     * @param event
     */
    synchronized public void process(WatchedEvent event) {
        System.out.println("ZooKeeper event " + event);

        try {
            List<String> children = _curClient.getChildren().usingWatcher(this).forPath("/gla");
            System.out.println("num children: " + children.size());
            classifyNode(children.size());
            if (_kvHandler.getRole() == KeyValueHandler.ROLE.BACKUP) {
                _kvHandler.fetchDataDump();
            }

            if (children.size() > 1) {
                _kvHandler.setAlone(false);
                String siblingName = getSiblingNode(_connectionString, children);
                String siblingHost = siblingName.split(":")[0];
                String siblingPort = siblingName.split(":")[1];
                connectToSibling(siblingHost, siblingPort);
            } else {
                _kvHandler.setAlone(true);
            }


        } catch (Exception e) {
            System.out.println("Unable to determine primary " + e);
        }

    }

    private void connectToSibling(String host, String port) {
        try {
            TSocket sock = new TSocket(host, Integer.valueOf(port));
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            _siblingClient = new KeyValueService.Client(protocol);
        } catch (Exception e) {
            System.out.println("Unable to connect to primary");
            e.printStackTrace();
        }
	    try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            System.out.println("Unable to sleep");

        }

    }

}
