import io.atomix.copycat.Command;

public class FAICommand implements Command<Long> {
    String _key;

    public FAICommand(String key) {
        _key = key;
    }
}
