import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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
//        System.out.println("get called " + key + " " + myMap.get(key));

        String ret = myMap.get(key);

        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
//        System.out.println("put called " + key + " " + value);
        myMap.put(key, value);

        if (_role.equals(ROLE.PRIMARY) && !_alone) {
            forwardData(key, value, _sequence.addAndGet(1));
        }
    }

    public void forward(String key, String value, int sequence) {
//        System.out.println("forward called " + key + " " + value);
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

    public void forwardData(String key, String value, int seq) throws org.apache.thrift.TException {
        ThriftClient tClient = null;
        try {
            tClient = ClientUtility.getAvailable();
            tClient.forward(key, value, seq);
        } catch (org.apache.thrift.TException | InterruptedException ex) {
            ex.printStackTrace();
            tClient.closeTransport();
            tClient = ClientUtility.generateRPCClient(tClient._host, tClient._port);
        } finally {
            if (tClient != null) {
                ClientUtility.makeAvailable(tClient);
            }
        }
    }

    public void transferMap() throws org.apache.thrift.TException {
        System.out.println("Transferring map to backup");
        Instant start = Instant.now();
//        System.out.println("ORIGINAL MAP -----");
//        for (Map.Entry<String, String> entry: myMap.entrySet()) {
//            System.out.println("original map: key: " + entry.getKey() + " value: " + entry.getValue());
//        }

        List<String> keys = new ArrayList<String>(myMap.keySet());
        List<String> values = new ArrayList<String>(keys.size());
        for(int i = 0; i < keys.size(); i++) {
            values.add(i, myMap.get(keys.get(i)));
        }
//        System.out.println("NEW MAP ------------- " + Duration.between(Instant.now(), start).toMillis());
//        for (int i = 0; i < keys.size(); i ++) {
//            System.out.println("new map: key: " + keys.get(i) + " value: " + values.get(i));
//        }

        if (myMap.size() > MAX_MAP_SIZE) {
            System.out.println("Sending map in multiple chunks");
            System.out.println("map size: " + keys.size());
            int index = 0;
            int end = 0;
            while(end != keys.size()) {
                end = Math.min(index + MAX_MAP_SIZE, keys.size());
                System.out.println("index: " + index + " end: " + end);
                setSiblingMap(keys.subList(index, end), values.subList(index, end));
                index = end;
            }
        } else {
            System.out.println("Sending map in single chunk");
            setSiblingMap(keys, values);
        }

        System.out.println("Time required to send map: " + Duration.between(start, Instant.now()).toMillis());
    }

    public void setSiblingMap(List<String> keys, List<String> values) {
//        System.out.println("SENDING -----------");
//        for (int i = 0; i < keys.size(); i ++) {
//            System.out.println("sending: key: " + keys.get(i) + " value: " + values.get(i));
//        }

        int maxRetry = 10;
        ThriftClient tClient = null;

        while (maxRetry > 0) {
            try {
                tClient = ClientUtility.getAvailable();
                tClient.setMyMap(keys, values);
                return;

            } catch (org.apache.thrift.TException | InterruptedException ex) {
                ex.printStackTrace();

                try {
                    Thread.currentThread().sleep(100);
                } catch (InterruptedException e) {
                    System.out.println("failed to sleep");
                    e.printStackTrace();
                }
                maxRetry--;
            } finally {
                if (tClient != null) {
                    ClientUtility.makeAvailable(tClient);
                }
            }
        }
        System.out.println("Failed to transfer map");
    }


    public void setMyMap(List<String> keys, List<String> values) {
        System.out.println("Setting " + keys.size() + " values to MyMap");
//        for (int i = 0; i < keys.size(); i ++) {
//            System.out.println("setMyMap: key: " + keys.get(i) + " value: " + values.get(i));
//        }

        for (int i = 0; i < keys.size(); i++) {
            if (!myMap.containsKey(keys.get(i))) {
//                System.out.println("actually setting: key: " + keys.get(i) + " value: " + values.get(i));
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
