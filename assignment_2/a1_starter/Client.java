import java.util.List;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class Client {
    public static void main(String [] args) {

//		// a list of indexes to available BENodes
//		ArrayList<Boolean> availableNodes = new ArrayList<>();
//		availableNodes.add(Boolean.FALSE);
//		System.out.println(availableNodes.size());
//		availableNodes.remove(0);
//
//		System.out.println(availableNodes.size());

		try {
			TSocket sock = new TSocket("localhost", 10000);
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);
			transport.open();

			// Generate a bunch of random passwords
			List<String> passwords = new ArrayList<>();
			passwords.add("asdf");
			passwords.add("sfiahfhis");
			passwords.add("sfishf");
			passwords.add("sdifsif");
			passwords.add("sfisjf");


			List<String> hashes = client.hashPassword(passwords, (short)10);

			for (int i = 0; i < hashes.size(); i++ ) {
				System.out.println("Hash " + hashes.get(i));
			}

			System.out.println("Positive check: " + client.checkPassword(passwords, hashes));
			hashes.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
			System.out.println("Negative check: " + client.checkPassword(passwords, hashes));
			hashes.set(0, "too short");
			System.out.println("Exception check: " + client.checkPassword(passwords, hashes));

			transport.close();
		} catch (TException x) {
			x.printStackTrace();
		}
    }
}
