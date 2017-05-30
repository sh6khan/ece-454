import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class SuperClient {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java Client FE_host FE_port");
            System.exit(-1);
        }

        IntStream.range(0, 20).parallel().forEach(i -> {

            try {
                TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                BcryptService.Client client = new BcryptService.Client(protocol);
                transport.open();

                List<String> password = new ArrayList<>();
                for (int j = 0; j < 50; j++) {
                    password.add("test" + j);
                }
                List<String> hash = client.hashPassword(password, (short) 10);
                System.out.println("Hash: " + hash.get(0) + ", " + hash.get(1));
                System.out.println("Positive check: " + client.checkPassword(password, hash));
                hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
                System.out.println("Negative check: " + client.checkPassword(password, hash));
                hash.set(0, "too short");
                System.out.println("Exception check: " + client.checkPassword(password, hash));


                transport.close();
            } catch (TException x) {
                x.printStackTrace();
            }
        });
    }
}
