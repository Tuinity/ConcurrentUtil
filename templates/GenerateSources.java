import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class GenerateSources {

    private static String firstUpper(final String value) {
        return Character.toUpperCase(value.charAt(0)) + value.toLowerCase(Locale.ROOT).substring(1);
    }

    private static final Map<Class<?>, Class<?>> BOX_MAPPING = new HashMap<>(
            Map.of(
                    boolean.class, Boolean.class,
                    byte.class, Byte.class,
                    short.class, Short.class,
                    char.class, Character.class,
                    int.class, Integer.class,
                    float.class, Float.class,
                    long.class, Long.class,
                    double.class, Double.class,
                    void.class, Void.class
            )
    );

    private static void defMacro(final String name, final List<String> args, final String body, final LinkedHashMap<String, Preprocessor.Macro> macros) {
        macros.put(name, new Preprocessor.Macro(name, args, body));
    }

    private static void doMap(final String dstDir, final Class<?> key, final Class<?> value,
                              final boolean isRefKey, final boolean isRefValue,
                              final Path src) throws Exception {
        final Path dst = Path.of(
                dstDir, "src", "main", "java", "ca", "spottedleaf", "concurrentutil", "map", "concurrent",
                key.getSimpleName().toLowerCase(Locale.ROOT) + "s",
                "ConcurrentChained" +
                        (isRefKey ? "Reference" : firstUpper(key.getSimpleName())) +
                        "2" +
                        (isRefValue ? "Reference" : firstUpper(value.getSimpleName())) +
                        "HashTable.java"
        );

        final List<Class<?>> classes = Arrays.asList(
                key, value
        );

        final LinkedHashMap<String, Preprocessor.Macro> macros = new LinkedHashMap<>();

        defMacro("__INPUT_TYPE_COUNT", new ArrayList<>(), Integer.toString(classes.size()), macros);
        for (int i = 0; i < classes.size(); ++i) {
            final Class<?> clazz = classes.get(i);

            defMacro("__INPUT_TYPE" + i, new ArrayList<>(), clazz.getSimpleName(), macros);
            defMacro("__INPUT_TYPE" + i + "_LOWERCASE", new ArrayList<>(), clazz.getSimpleName().toLowerCase(Locale.ROOT), macros);
            defMacro("__INPUT_TYPE" + i + "_FIRST_UPPER", new ArrayList<>(), firstUpper(clazz.getSimpleName()), macros);

            if (clazz.isPrimitive()) {
                defMacro("__INPUT_TYPE" + i + "_PRIMITIVE", new ArrayList<>(), "1", macros);

                final boolean number = clazz != void.class && clazz != boolean.class;
                defMacro("__INPUT_TYPE" + i + "_IS_NUMBER", new ArrayList<>(), number ? "1" : "0", macros);

                final Class<?> boxed = BOX_MAPPING.get(clazz);

                defMacro("__INPUT_TYPE" + i + "_BOXED", new ArrayList<>(), boxed.getSimpleName(), macros);
                defMacro("__INPUT_TYPE" + i + "_BOX_FUNCTION",
                        new ArrayList<>(
                                Arrays.asList(
                                        "_X"
                                )
                        ),
                        "(" + boxed.getSimpleName() + ".valueOf(_X))",
                        macros
                );
                defMacro("__INPUT_TYPE" + i + "_UNBOX_FUNCTION",
                        new ArrayList<>(
                                Arrays.asList(
                                        "_X"
                                )
                        ),
                        "(_X." + clazz.getCanonicalName() + "Value())",
                        macros
                );
            } else {
                defMacro("__INPUT_TYPE" + i + "_PRIMITIVE", new ArrayList<>(), "0", macros);
            }
        }

        defMacro("IS_REFERENCE_KEY", new ArrayList<>(), isRefKey ? "1" : "0", macros);
        defMacro("IS_REFERENCE_VALUE", new ArrayList<>(), isRefValue ? "1" : "0", macros);

        Files.deleteIfExists(dst);

        if (dst.getParent() != null) {
            Files.createDirectories(dst.getParent());
        }

        Files.write(
                dst, Preprocessor.parse(src, macros), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE
        );

        System.out.println("Generated " + dst + " from " + src);
    }

    public static void main(final String[] args) throws Exception {
        final Path src = Path.of(args[0], "ConcurrentChainedHashTable.java.txt");

        final List<ClassType> types = Arrays.asList(
                new ClassType(int.class, false),
                new ClassType(long.class, false),
                new ClassType(Object.class, false),
                new ClassType(Object.class, true)
        );

        for (final ClassType key : types) {
            for (final ClassType value : types) {
                doMap(args[1], key.clazz, value.clazz, key.isReferenceEquality, value.isReferenceEquality, src);
            }
        }
    }

    private static final record ClassType(Class<?> clazz, boolean isReferenceEquality) {}
}
