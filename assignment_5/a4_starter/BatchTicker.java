import io.atomix.copycat.client.CopycatClient;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BatchTicker implements Runnable {
    private static final int WAIT_TIME = 10;
    private CopycatClient _client;
    private ReentrantReadWriteLock _lock;

    public BatchTicker(String ccHost, int ccPort, ReentrantReadWriteLock lock) {
        _client = CopycatClientFactory.buildCopycatClient(ccHost, ccPort);
        _lock = lock;

        System.out.println("BatchTicker got copy cat client");
    }

    public void run() {
        while(true) {
            try {
                Thread.sleep(WAIT_TIME);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                System.out.println("nothing to see here folks");
            }


            CommandBuffer.commit(_client, _lock);
        }
    }
}
