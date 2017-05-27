import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    public List<String> hashPassword(List<String> passwords, short logRounds) throws IllegalArgument, org.apache.thrift.TException
    {

        try {
            List<String> ret = new ArrayList<>();

            String hashedPassword;
            for (String password : passwords ) {
                hashedPassword = BCrypt.hashpw(password, BCrypt.gensalt(logRounds));
                ret.add(hashedPassword);
            }

            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }

    public List<Boolean> checkPassword(List<String> passwords, List<String> hashes) throws IllegalArgument, org.apache.thrift.TException
    {
        try {

            // throw error if the size of hashes
            if (passwords.size() != hashes.size()) {
                throw new Exception("passwords and hashes are not equal. bitch, wtf you trying to do here?");
            }

            List<Boolean> ret = new ArrayList<>();

            String password;
            String hash;
            for (int i = 0; i < passwords.size(); i++) {
                password = passwords.get(i);
                hash = hashes.get(i);
                ret.add(BCrypt.checkpw(password, hash));
            }

            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }
}
