import io.atomix.copycat.client.CopycatClient;

public class BatchTicker implements Runnable {
    private static final int WAIT_TIME = 5;
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

            // commits can be started by other threads, we don't want to trigge
            // a commit while its already doing so.
            if (CommandBuffer.state.equals(CommandBuffer.STATE.COMITTING)) {
                System.out.println("Not Timed Commit");
                continue;
            }


            CommandBuffer.commit(_client);
        }
    }
}
