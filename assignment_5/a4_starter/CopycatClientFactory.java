import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;

import java.util.ArrayList;
import java.util.List;

public class CopycatClientFactory {
    public static CopycatClient buildCopycatClient(String ccHost, int ccPort) {

        CopycatClient client;

        List<Address> members = new ArrayList<>();
        members.add(new Address(ccHost, ccPort));
        client = CopycatClient.builder()
                .withTransport(new NettyTransport())
                .build();

        client.serializer().register(GetQuery.class, 1);
        client.serializer().register(FAICommand.class, 2);
        client.serializer().register(FADCommand.class, 3);
        client.serializer().register(BatchCommand.class, 4);
        client.connect(members).join();

        return client;
    }
}
