import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedClient {
    private static ExecutorService _executorService = Executors.newCachedThreadPool(new ThreadFactory() {
        private AtomicInteger threadCounter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);
            t.setName("client-thread-" + threadCounter.incrementAndGet());
            return t;
        }
    });

    public static void main(String [] args) {
        SimpleClient simpleClient = new SimpleClient(args[0], Integer.parseInt(args[1]));
        BigClient bigClient = new BigClient(args[0], Integer.parseInt(args[1]));

        _executorService.submit(simpleClient);
        _executorService.submit(bigClient);
    }
}

class SimpleClient implements Runnable {
    private String _hostname;
    private int _port;

    public SimpleClient (String hostname, int port) {
        _hostname = hostname;
        _port = port;
    }

    /**
     * Continuously send requests to the FENode with simple password lists
     * wait 3 seconds in between
     */
    @Override
    public void run() {
        while (true) {
            try {
                TSocket sock = new TSocket(_hostname, _port);
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client client = new BcryptService.Client(protocol);
                transport.open();

                Instant startTime = Instant.now();

                List<String> passwords = genPasswordList();
                List<String> hashes = client.hashPassword(passwords, (short) 10);

                System.out.println("size: " + passwords.size() + " took: " + Duration.between(Instant.now(), startTime).toMillis());
                //System.out.println("Check: " + client.checkPassword(passwords, hashes));

                transport.close();
            } catch (TException x) {
                System.out.println(x.getMessage());
            }

            try {
                Thread.sleep(3000);
            } catch (Exception ex) {
                // do nothing
            }

        }
    }

    private List<String> genPasswordList() {
        // Generate a bunch of random passwords
        List<String> passwords = new ArrayList<>();
        passwords.add("asdf");
        passwords.add("sfiahfhis");
        passwords.add("sfishf");
        passwords.add("sdifsif");
        passwords.add("sfisjf");

        return passwords;
    }
}


class BigClient implements Runnable {
    private String _hostname;
    private int _port;

    public BigClient (String hostname, int port) {
        _hostname = hostname;
        _port = port;
    }

    /**
     * Continuously send requests to the FENode with heavy password lists
     * wait 5 seconds in between
     */
    @Override
    public void run() {
        while (true) {
            try {
                TSocket sock = new TSocket(_hostname, _port);
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client client = new BcryptService.Client(protocol);
                transport.open();

                List<String> passwords = genPasswordList();
                Instant startTime = Instant.now();

                List<String> hashes = client.hashPassword(passwords, (short) 5);

                System.out.println("size: " + passwords.size() + " took: " + Duration.between(Instant.now(), startTime).toMillis());
                System.out.println("Check: " + client.checkPassword(passwords, hashes));

                transport.close();
            } catch (TException x) {
                System.out.println(x.getMessage());
            }

            try {
                Thread.sleep(3000);
            } catch (Exception ex) {
                // do nothing
            }

        }
    }

    public static List<String> genPasswordList() {
        List<String> l = new ArrayList<String>(1024);
        String somebigpassword = "faldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurvcvmvcmdoiZZ";
        for (int i = 0; i < 4; i++) {
            l.add(somebigpassword + i);
        }
        return l;
    }
}