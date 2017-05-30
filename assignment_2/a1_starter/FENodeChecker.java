import org.apache.thrift.transport.TTransport;

import java.time.Duration;

public class FENodeChecker implements Runnable {
    private TTransport _transport;
    private BcryptService.Client _FENodeClient;
    private final String _hostname;
    private final String _port;

    private static final int MAX_ATTEMPTS = 50;
    private static final Duration RETRY_WAIT_TIME = Duration.ofSeconds(4);

    public FENodeChecker (TTransport transport,
                          BcryptService.Client client,
                          String hostname,
                          String port) {
        _transport = transport;
        _FENodeClient = client;

        _port = port;
        _hostname = hostname;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(BatchTracker.TIMEOUT.toMillis());
            } catch (Exception e) {
                System.out.println("interrupted thread on FENodeChecker");
                System.out.println(e.getMessage());
                e.printStackTrace();
                // do nothing?
            }

            if (BatchTracker.isFENodeDown()) {
                System.out.println("FENode is down!");
                establishConnectionToFENode();
            }
        }
    }

    /**
     * Continously try an establish a connection to the FENode
     */
    public void establishConnectionToFENode() {
        int currentAttempt = 0;

        while (currentAttempt < MAX_ATTEMPTS) {
            try {
                System.out.println("trying to connect to FENode: " + _hostname + " " + _port);

                _transport.open();
                _FENodeClient.heartBeat(_hostname, _port);
                _transport.close();

                System.out.println("Successfully found FENode");
                return;
            } catch (Exception e) {
                // wait for a while and then keep retrying
                currentAttempt++;
                try {
                    Thread.sleep(RETRY_WAIT_TIME.toMillis());
                } catch (Exception ex) {
                    // do nothing
                    System.out.println("error while trying to re open connection");
                }
            } finally {
                if(_transport.isOpen()){
                    _transport.close();
                }

            }
        }

        System.out.println("Failed to connect " + MAX_ATTEMPTS + " times to the FENode");
    }


}
