import org.apache.thrift.transport.TTransport;

/**
 * A class representing a BENode info
 */
public class NodeInfo {
    private BcryptService.Client _BENodeClient;
    private TTransport _transport;
    private boolean _occupied = false;
    public String nodeId;

    NodeInfo(BcryptService.Client client, TTransport transport, String id) {
        _BENodeClient = client;
        _transport = transport;
        _occupied = false;
        this.nodeId = id;
    }

    public BcryptService.Client getClient() {
        return _BENodeClient;
    }

    public TTransport getTransport() {
        return _transport;
    }

    public boolean isNotOccupied() {
        return !_occupied;
    }

    public void markOccupied() {
        _occupied = true;
    }

    public void markAvailable() {
        _occupied = false;
    }
}
