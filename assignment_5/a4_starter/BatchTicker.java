import io.atomix.copycat.client.CopycatClient;

public class BatchTicker implements Runnable {
    private static final int WAIT_TIME = 10;
    private CopycatClient _client;

    public BatchTicker(String ccHost, int ccPort) {
        _client = CopycatClientFactory.buildCopycatClient(ccHost, ccPort);
        System.out.println("BatchTicker got copy cat client");
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

            synchronized (this) {
                CommandBuffer.commit(_client);
            }
        }
    }
}
