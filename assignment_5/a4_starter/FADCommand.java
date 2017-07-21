import io.atomix.copycat.Command;


public class FADCommand implements Command<Long> {
    String _key;

    public FADCommand(String key) {
        _key = key;
    }
}
