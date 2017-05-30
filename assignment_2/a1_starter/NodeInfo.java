import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * A class representing a BENode info
 */

public class NodeInfo {
    private BcryptService.Client _BENodeClient;
    private TTransport _transport;
    private final String _hostname;
    private final String _port;
    private double _load;
    private boolean _occupied;

    public String nodeId;

    NodeInfo(String hostname, String port) {

        TSocket sock = new TSocket(hostname, Integer.parseInt(port));
        TTransport transport = new TFramedTransport(sock);
        TProtocol protocol = new TBinaryProtocol(transport);
        BcryptService.Client client = new BcryptService.Client(protocol);

        _BENodeClient = client;
        _transport = transport;

        this.nodeId = hostname + port;
        _occupied = false;

        _hostname = hostname;
        _port = port;
    }

    public BcryptService.Client getClient() {
        return _BENodeClient;
    }

    public TTransport getTransport() {
        return _transport;
    }

    public void markOccupied() {
        _occupied = true;
    }

    public void markAvailable() {
        _occupied = false;
    }

    public boolean isNotOccupied() {
        return !_occupied;
    }

    public void addLoad(int numPasswords, short logRounds) {
        _load += numPasswords * Math.pow(2, logRounds);
    }

    public void reduceLoad(int numPasswords, short logRounds) {
        _load -= numPasswords * Math.pow(2, logRounds);
    }

    public double getLoad() {
        return _load;
    }

    public String getHostname() {
        return _hostname;
    }

    public String getPort() {
        return _port;
    }
}
