import java.util.function.Consumer;
import java.util.stream.Stream;

public class Main {

    private static class Action {
        private String name;
        private Consumer<String[]> run;

        public Action(String name, Consumer<String[]> run) {
            this.name = name;
            this.run = run;
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Main <action>");
            System.exit(1);
        }

        Action[] actions = {
                new Action("formatted", Formatted::run),
                new Action("exploitation", Exploitation::run),
                new Action("model", Model::run),
                new Action("streaming", Streaming::run),
        };

        for (Action action : actions) {
            if (action.name.startsWith(args[0])) {
                System.out.println("Running action: " + action.name);
                action.run.accept(args);
                return;
            }
        }

        System.out.println("Unknown action: " + args[0]);
        System.out.println("Available actions: "
                + Stream.of(actions).map(a -> a.name).collect(java.util.stream.Collectors.joining(", ")));
        System.exit(1);
    }
}
