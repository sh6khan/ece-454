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

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);
        //return "1";

        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        // System.out.println("put called " + key + " " + value);
        myMap.put(key, value);

        if (_role.equals(ROLE.PRIMARY) && !_alone) {
            forwardData(key, value);
        }
    }

    public void forwardData(String key, String value) throws org.apache.thrift.TException {
        KeyValueService.Client _siblingClient = null;
        try {
             _siblingClient = ClientUtility.getAvailable();
            _siblingClient.put(key, value);
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            if (_siblingClient != null) {
                ClientUtility.makeAvailable(_siblingClient);
            }
        }

    }

    public void fetchDataDump() throws org.apache.thrift.TException {
        System.out.println("copying data from primary");

        if (!_role.equals(ROLE.BACKUP)) {
            throw new RuntimeException(String.format("Should only be called by BACKUP, called by: ", _role));
        }

        KeyValueService.Client _siblingClient = null;

        try {
            _siblingClient = ClientUtility.getAvailable();
            Map<String, String> tempMap = _siblingClient.getDataDump();
            for (String key : tempMap.keySet()) {
                if (!myMap.containsKey(key)) {
                    myMap.put(key, tempMap.get(key));
                }
            }
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            if (_siblingClient != null) {
                ClientUtility.makeAvailable(_siblingClient);
            }
        }
    }

    public Map<String, String> getDataDump() throws org.apache.thrift.TException {
        if (!_role.equals(ROLE.PRIMARY)) {
            throw new RuntimeException(String.format("Should only be implemented by PRIMARY, implemented by: %s", _role));
        }

        return myMap;
    }
}
