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
    private static final int MAX_MAP_SIZE = 200000;


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
        ThriftClient tClient = null;
        try {
            tClient = ClientUtility.getAvailable();
            tClient.put(key, value);
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            if (tClient != null) {
                ClientUtility.makeAvailable(tClient);
            }
        }
    }

    public void transferMap() throws org.apache.thrift.TException {
        List<String> keys = new ArrayList<String>(myMap.keySet());
        List<String> values = new ArrayList<String>(myMap.values());

        if (myMap.size() > MAX_MAP_SIZE) {
            int index = 0;
            int end = 0;
            while(end != keys.size()) {
                end = Math.min(index + MAX_MAP_SIZE, keys.size());
                setSiblingMap(keys.subList(index, end), values.subList(index, end));
                index = end + 1;
            }
        } else {
            setSiblingMap(keys, values);
        }
    }

    public void setSiblingMap(List<String> keys, List<String> values) {
        ThriftClient tClient = null;
        try {
            tClient = ClientUtility.getAvailable();
            tClient.setMyMap(keys, values);
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            if (tClient != null) {
                ClientUtility.makeAvailable(tClient);
            }
        }
    }

    public void setMyMap(List<String> keys, List<String> values) {
        for (int i = 0; i < keys.size(); i++) {
            if (!myMap.containsKey(keys.get(i))) {
                myMap.put(keys.get(i), values.get(i));
            }
        }
    }

    public void fetchDataDump() throws org.apache.thrift.TException {
        System.out.println("copying data from primary");

        if (!_role.equals(ROLE.BACKUP)) {
            throw new RuntimeException(String.format("Should only be called by BACKUP, called by: ", _role));
        }

        ThriftClient tClient = null;

        try {
            tClient = ClientUtility.getAvailable();
            Map<String, String> tempMap = tClient.getDataDump();
            for (String key : tempMap.keySet()) {
                if (!myMap.containsKey(key)) {
                    myMap.put(key, tempMap.get(key));
                }
            }
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            if (tClient != null) {
                ClientUtility.makeAvailable(tClient);
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
