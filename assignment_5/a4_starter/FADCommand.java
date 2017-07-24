import io.atomix.copycat.Command;


public class FADCommand implements Command<Long> {
    String _key;
    int _delta;

    public FADCommand(String key, int delta) {
        _key = key;
        _delta = delta;
    }
}
