import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private KeyValueService.Client _siblingClient;
    private String zkNode;
    private String host;
    private int port;
    private ROLE _role = ROLE.UNDEFINED;
    private boolean _alone = true;


    enum ROLE {
        PRIMARY,
        BACKUP,
        UNDEFINED
    }

    public void setRole(ROLE newRole) {
        _role = newRole;
    }

    public ROLE getRole() {
        return _role;
    }

    public void setAlone(boolean alone) {
        _alone = alone;
    }

    public String getZkNode() {
        return zkNode;
    }

    public void setSiblingClient(KeyValueService.Client newClient) {
        //TODO close previous client
        _siblingClient = newClient;
    }

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);

        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        System.out.println("put called " + key + " " + value);
        myMap.put(key, value);

        if (_role.equals(ROLE.PRIMARY) && !_alone) {
            forwardData(key, value);
        }
    }

    public synchronized void forwardData(String key, String value) throws org.apache.thrift.TException {
        _siblingClient.put(key, value);
    }

    public void fetchDataDump() throws org.apache.thrift.TException {
        System.out.println("copying data from primary");

        if (!_role.equals(ROLE.BACKUP)) {
            throw new RuntimeException(String.format("Should only be called by BACKUP, called by: ", _role));
        }

        myMap = _siblingClient.getDataDump();
    }

    public Map<String, String> getDataDump() throws org.apache.thrift.TException {
        if (!_role.equals(ROLE.PRIMARY)) {
            throw new RuntimeException(String.format("Should only be implemented by PRIMARY, implemented by: ", _role));
        }

        return myMap;
    }
}
