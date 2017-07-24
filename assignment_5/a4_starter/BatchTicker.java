import io.atomix.copycat.client.CopycatClient;

public class BatchTicker implements Runnable {
    private static final int WAIT_TIME = 3;
    private CopycatClient _client;

    public BatchTicker(String ccHost, int ccPort) {
        _client = CopycatClientFactory.buildCopycatClient(ccHost, ccPort);

        if (_client != null) {
            System.out.println("BatchTicker got copy cat client");
        } else {
            System.out.println("BatchTicker Did NOT get cat client");
        }
    }

    public void run() {
        System.out.println("Starting batch ticker thread");

        while(true) {
            try {
                Thread.sleep(WAIT_TIME);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("nothing to see here folks");
            }

            CommandBuffer.commit(_client);
        }
    }
}