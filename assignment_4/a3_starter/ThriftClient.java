import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

/**
 * A simple wrapper around the Thrift client which stores both the Transport
 * and KeyValueService.Client
 *
 * This wrapper class allows us to close the transport through the {@code closeTransport() }
 */
public class ThriftClient {
    public String _host;
    public Integer _port;
    private KeyValueService.Client _client;
    private TTransport _transport;

    public ThriftClient(KeyValueService.Client client, TTransport transport, String host, Integer port) {
        _client = client;
        _transport = transport;
        _host = host;
        _port = port;
    }

    public void forward(String key, String value, int sequence) throws TException {
        _client.forward(key, value, sequence);
    }

    public String get(String key) throws TException {
        return _client.get(key);
    }

    public void setMyMap(List<String> keys, List<String> values) throws TException{
        _client.setMyMap(keys, values);
    }

    public void closeTransport() {
        _transport.close();
    }
}
