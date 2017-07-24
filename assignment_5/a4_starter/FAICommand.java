import io.atomix.copycat.Command;

public class FAICommand implements Command<Long> {
    String _key;
    int _delta;

    public FAICommand(String key, int delta) {
        _key = key;
        _delta = delta;
    }
}
