import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
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
    private String _zkName;

    public NodeWatcher(CuratorFramework curClient, KeyValueHandler handler, String zkName) {
        _curClient = curClient;
        _kvHandler = handler;
        _zkName = zkName;
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

    /**
     * Callback function on the watcher
     *
     * @param event
     */
    synchronized public void process(WatchedEvent event) {
        System.out.println("ZooKeeper event: " + event);

        try {
            List<String> children = _curClient.getChildren().usingWatcher(this).forPath(_zkName);
            Collections.sort(children);
            System.out.println("num children: " + children.size());
            classifyNode(children.size());

            if (children.size() > 1) {
                System.out.println("size greater than 1 and role: " + _kvHandler.getRole());
                InetSocketAddress address = ClientUtility.extractSiblingInfo(children, _kvHandler.getZkNode(), _kvHandler.getRole(), _curClient);
                int cap = _kvHandler.getRole().equals(KeyValueHandler.ROLE.BACKUP) ? ClientUtility.BACKUP_POOL_NUM : ClientUtility.PRIMARY_POOL_NUM;
                ClientUtility.populateClientObjectPool(address.getHostName(), address.getPort(), cap);
                _kvHandler.setAlone(false);
            } else {
                _kvHandler.setAlone(true);
            }

        } catch (Exception e) {
            System.out.println("Unable to determine primary " + e);
        }
        
        

    }
}
