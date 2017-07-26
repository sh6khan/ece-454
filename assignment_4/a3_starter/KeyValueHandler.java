import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.*;


public class KeyValueHandler implements KeyValueService.Iface {
    private Map<String, String> myMap;
    private Map<String, Integer> sequenceMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private ROLE _role = ROLE.UNDEFINED;
    private boolean _alone = true;
    private static AtomicInteger _sequence;
    private static final int MAX_MAP_SIZE = 500000;


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
        sequenceMap = new ConcurrentHashMap<String, Integer>();
        myMap = new ConcurrentHashMap<String, String>();
        _sequence = new AtomicInteger(0);
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);

        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        myMap.put(key, value);

        if (_role.equals(ROLE.PRIMARY) && !_alone) {
            forwardData(key, value, _sequence.addAndGet(1));
        }
    }

    /**
     * Forwards the request to the BACKUP node
     *
     * @param sequence - value to validate whether the transferred values is the most recent value
     */
    public void forward(String key, String value, int sequence) {
        if (sequenceMap.containsKey(key)) {
            if (sequence >= sequenceMap.get(key)) {
                myMap.put(key, value);
                sequenceMap.put(key, sequence);
            }
        } else {
            myMap.put(key, value);
            sequenceMap.put(key, sequence);
        }
    }

    /**
     *Get a client from the pool, forward request to BACKUP node
     *
     * @param 
     */
    public void forwardData(String key, String value, int seq) throws org.apache.thrift.TException {
        ThriftClient tClient = null;
        try {
            tClient = ClientUtility.getAvailable();
            tClient.forward(key, value, seq);
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
            tClient = ClientUtility.generateRPCClient(tClient._host, tClient._port);
        } finally {
            if (tClient != null) {
                ClientUtility.makeAvailable(tClient);
            }
        }
    }

    /**
     *Copy map onto BACKUP node, split map into chunks if it exceeds TFrame size limit
     *
     * @param
     */
    public void transferMap() throws org.apache.thrift.TException {
        List<String> keys = new ArrayList<String>(myMap.keySet());
        List<String> values = new ArrayList<String>(keys.size());
        for(int i = 0; i < keys.size(); i++) {
            values.add(i, myMap.get(keys.get(i)));
        }

        if (myMap.size() > MAX_MAP_SIZE) {
            int index = 0;
            int end = 0;
            while(end != keys.size()) {
                end = Math.min(index + MAX_MAP_SIZE, keys.size());
                setSiblingMap(keys.subList(index, end), values.subList(index, end));
                index = end;
            }
        } else {
            setSiblingMap(keys, values);
        }
    }

    /**
     *Gets a client from the pool, sends RPC to copy map onto BACKUP
     *
     * @param
     */
    public void setSiblingMap(List<String> keys, List<String> values) {
        ThriftClient tClient = null;
        try {
            tClient = ClientUtility.getAvailable();
            tClient.setMyMap(keys, values);
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            System.out.println(ex.getMessage());
            ex.printStackTrace();
            
        } finally {
            if (tClient != null) {
                ClientUtility.makeAvailable(tClient);
            }
        }
    }

    /**
     *Sets keys and values into the map
     *
     * @param
     */
    public void setMyMap(List<String> keys, List<String> values) {
        for (int i = 0; i < keys.size(); i++) {
            if (!myMap.containsKey(keys.get(i))) {
                myMap.put(keys.get(i), values.get(i));
            }
        }
    }
}
