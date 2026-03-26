import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Preprocessor {

    private static List<String> readInclude(final Path file, final String include) throws IOException {
        return parseLineBreaks(Files.readAllLines(file.resolveSibling(include)));
    }

    public static List<String> parse(final Path file, final LinkedHashMap<String, Macro> macros) throws Exception {
        return trim(processLines(file, Files.readAllLines(file), macros));
    }

    private static List<String> trim(final List<String> list) {
        int emptyHeaderLines = 0;
        int emptyFooterLines = 0;

        for (; emptyHeaderLines < list.size(); ++emptyHeaderLines) {
            if (!list.get(emptyHeaderLines).isBlank()) {
                break;
            }
        }

        if (emptyHeaderLines == list.size()) {
            return new ArrayList<>();
        }

        for (; emptyFooterLines < list.size(); ++emptyFooterLines) {
            if (!list.get(list.size() - emptyFooterLines - 1).isBlank()) {
                break;
            }
        }

        return new ArrayList<>(list.subList(emptyHeaderLines, list.size() - emptyFooterLines));
    }

    private static List<String> parseLineBreaks(final List<String> lines) {
        final List<String> ret = new ArrayList<>();
        final StringBuilder currentLine = new StringBuilder();

        for (final String line : lines) {
            if (currentLine.isEmpty()) {
                // not parsing a line

                final int endIdx;
                if (line.startsWith("#") && (endIdx = line.indexOf('\\')) != -1) {
                    // begin parsing
                    currentLine.append(line, 0, endIdx);
                } else {
                    ret.add(line);
                }
                continue;
            }

            final int endIdx = line.indexOf('\\');
            if (endIdx == -1) {
                // done parsing
                currentLine.append(line);
                ret.add(currentLine.toString());
                currentLine.setLength(0);
                continue;
            }

            currentLine.append(line, 0, endIdx);
        }

        if (!currentLine.isEmpty()) {
            ret.add(currentLine.toString());
        }

        return ret;
    }

    private static List<String> processLines(final Path currentFile, final List<String> rawLines,
                                             final Map<String, Macro> macros) throws Exception {
        final List<String> ret = new ArrayList<>();

        final ArrayDeque<String> virtualLines = new ArrayDeque<>(parseLineBreaks(rawLines));

        final LinkedList<IfState> ifStack = new LinkedList<>();

        while (!virtualLines.isEmpty()) {
            final String line = virtualLines.pollFirst();
            // force ifs to evaluate to false if we are skipping
            final boolean evaluate = ifStack.isEmpty() || ifStack.peek().execute;
            if (line.startsWith("#")) {
                final String[] split = line.split("\\s+", 2);

                final String directiveName = split[0].substring(1);
                final String directiveValue = split.length > 1 ? split[1] : "";

                switch (directiveName) {
                    case "if": {
                        ifStack.push(new IfState(evaluate && evaluateCondition(directiveValue, macros)));
                        break;
                    }
                    case "ifdef": {
                        ifStack.push(new IfState(evaluate && macros.containsKey(directiveValue.trim())));
                        break;
                    }
                    case "ifndef": {
                        ifStack.push(new IfState(evaluate && !macros.containsKey(directiveValue.trim())));
                        break;
                    }
                    case "else":
                        if (ifStack.isEmpty()) {
                            throw new Exception("Unexpected else directive without matching if directive");
                        }

                        // remove old state
                        final IfState current = ifStack.pop();

                        // force evaluation to false if any are true:
                        // 1. parent is not executing
                        // 2. already executed branch
                        final boolean evalToFalse = !(ifStack.isEmpty() || ifStack.peek().execute) ||
                                current.branchExecuted;

                        final boolean nextCond;
                        if (directiveValue.startsWith("if ")) {
                            nextCond = !evalToFalse && evaluateCondition(directiveValue.substring("if ".length()).trim(), macros);
                        } else if (directiveValue.startsWith("ifdef ")) {
                            nextCond = !evalToFalse && macros.containsKey(directiveValue.substring("ifdef ".length()).trim());
                        } else if (directiveValue.startsWith("ifndef ")) {
                            nextCond = !evalToFalse && !macros.containsKey(directiveValue.substring("ifndef ".length()).trim());
                        } else if (directiveValue.isBlank()) {
                            // unconditional
                            nextCond = !evalToFalse && true;
                        } else {
                            throw new Exception("Unexpected value for else: " + directiveValue);
                        }

                        ifStack.push(new IfState(nextCond, current.branchExecuted | nextCond));
                        break;
                    case "endif": {
                        if (ifStack.isEmpty()) {
                            throw new Exception("Unexpected endif directive without matching if directive");
                        }
                        ifStack.pop();
                        break;
                    }
                    case "define": {
                        final Macro macro = parseMacro(directiveValue);

                        if (!evaluate) {
                            // make sure syntax checks were performed
                            break;
                        }

                        macros.put(macro.name, macro);
                        break;
                    }
                    case "error": {
                        if (!evaluate) {
                            break;
                        }
                        throw new Exception("Preprocessor error: " + directiveValue);
                    }
                    case "include": {
                        if (!evaluate) {
                            break;
                        }

                        // simply add the included lines
                        // note: recursion is going to OOM
                        for (final String include : readInclude(currentFile, directiveValue.replace("\"", "").trim()).reversed()) {
                            virtualLines.addFirst(include);
                        }

                        break;
                    }
                    case "comment": {
                        // comments are ignored
                        break;
                    }
                    default: {
                        throw new Exception("Invalid directive: " + directiveName);
                    }
                }
            } else {
                if (evaluate) {
                    String expanded = expandMacros(line, macros);
                    expanded = applyConcatenation(expanded);
                    ret.add(expanded);
                }
            }
        }

        if (!ifStack.isEmpty()) {
            throw new Exception("Missing #endif");
        }

        return ret;
    }

    private static Macro parseMacro(final String value) throws Exception {
        int nameEnd = -1;
        for (int i = 0; i < value.length(); ++i) {
            final char c = value.charAt(i);
            if (Character.isWhitespace(c) || c == '(') {
                nameEnd = i;
                break;
            }
        }

        if (nameEnd <= 0) {
            throw new Exception("Macro must have value definition: " + value);
        }

        final String name = value.substring(0, nameEnd);

        final int argsEnd;
        final List<String> args = new ArrayList<>();
        if (value.charAt(nameEnd) == '(') {
            argsEnd = value.indexOf(')', nameEnd + 1);
            if (argsEnd == -1) {
                throw new Exception("Macro args are invalid: "  + value);
            }
            // remove parentheses
            final String inner = value.substring(nameEnd + 1, argsEnd);
            if (!inner.isEmpty()) {
                for (final char c : inner.toCharArray()) {
                    if (Character.isWhitespace(c)) {
                        throw new Exception("Whitespace not allowed in macro args: " + value);
                    }
                }

                args.addAll(Arrays.asList(inner.split(",")));
            }
        } else {
            argsEnd = nameEnd - 1;
        }

        final String body;
        if (value.length() <= (argsEnd + 1)) {
            body = "";
        } else if (!Character.isWhitespace(value.charAt(argsEnd + 1))) {
            throw new Exception("Macro arguments must end followed by whitespace: " + value);
        } else {
            body = value.substring(argsEnd + 2);
        }

        return new Macro(name, args, body);
    }

    private static String expandMacros(String line, final Map<String, Macro> macros) throws Exception {
        boolean changed = true;
        while (changed) {
            changed = false;

            for (final Macro macro : macros.values()) {
                final Matcher matcher = macro.nameLiteral.matcher(line);
                if (macro.args.isEmpty()) {
                    // simple value macro, just replace all instances
                    if (matcher.find()) {
                        line = matcher.replaceAll(macro.bodyQuote);
                        changed = true;
                    }
                } else {
                    // note: we don't need to parse multiple instances here
                    final int nameIdx = line.indexOf(macro.name + "(");

                    if (nameIdx == -1) {
                        continue;
                    }

                    final List<String> passedArgs = new ArrayList<>();
                    final StringBuilder currentArg = new StringBuilder();

                    // We need handle composite macros
                    int depth = 1;
                    int argEnd = -1;
                    for (int i = nameIdx + macro.name.length() + 1; i < line.length(); i++) {
                        final char c = line.charAt(i);
                        if (c == '(') {
                            depth++;
                            currentArg.append(c);
                        } else if (c == ')') {
                            depth--;
                            if (depth == 0) {
                                // done parsing args
                                passedArgs.add(currentArg.toString());
                                argEnd = i;
                                break;
                            } else {
                                currentArg.append(c);
                            }
                        } else if (c == ',' && depth == 1) {
                            passedArgs.add(currentArg.toString());
                            currentArg.setLength(0);
                        } else {
                            currentArg.append(c);
                        }
                    }

                    if (passedArgs.size() != macro.args.size()) {
                        throw new Exception("Macro " + macro.name + " has " + macro.args.size() + " args, but attempting to pass " + passedArgs);
                    }

                    if (argEnd != -1) {
                        String expandedBody = macro.body;
                        for (int i = 0; i < macro.args.size(); i++) {
                            // replace all args
                            expandedBody = expandedBody.replaceAll(
                                    "\\b" + Pattern.quote(macro.args.get(i)) + "\\b",
                                    Matcher.quoteReplacement(passedArgs.get(i))
                            );
                        }
                        // replace macro in the line
                        line = line.substring(0, nameIdx) + expandedBody + line.substring(argEnd + 1);
                        changed = true;
                    }
                }
            }
        }

        return line;
    }

    private static String applyConcatenation(final String line) {
        return line.replace("##", "");
    }

    private static boolean evaluateCondition(final String expr, final Map<String, Macro> macros) throws Exception {
        // expand all macros so that the condition evaluator simply needs to test constants
        return new ConditionEvaluator(expandMacros(expr, macros)).evaluate();
    }

    public static record Macro(String name, List<String> args, String body, Pattern nameLiteral, String bodyQuote) {
        public Macro(final String name, final List<String> args, final String body) {
            this(name, args, body, Pattern.compile("\\b" + name + "\\b"), Matcher.quoteReplacement(body));
        }

        @Override
        public String toString() {
            return this.name + "(" + String.join(",", this.args) + ") " + this.body;
        }
    }

    private record IfState(boolean execute, boolean branchExecuted) {
        IfState(final boolean execute) {
            this(execute, execute);
        }
    }

    private static final class ConditionEvaluator {

        private final List<Token> tokens;
        private int pos;

        public ConditionEvaluator(final String str) {
            this.tokens = tokenize(str);
        }

        private static final Token[] NON_VALUE_TOKENS = new Token[] {
                new Token(TokenType.AND, "&&"),
                new Token(TokenType.OR, "||"),
                new Token(TokenType.EQ, "=="),
                new Token(TokenType.NEQ, "!="),
                new Token(TokenType.LPAREN, "("),
                new Token(TokenType.RPAREN, ")")
        };

        private static List<Token> tokenize(final String expr) {
            final List<Token> ret = new ArrayList<>();

            final StringBuilder currentValue = new StringBuilder();

            char_iteration:
            for (int i = 0; i < expr.length();) {
                final char c = expr.charAt(i);
                if (Character.isWhitespace(c)) {
                    ++i;
                    // terminate value token
                    if (!currentValue.isEmpty()) {
                        ret.add(new Token(TokenType.VALUE, currentValue.toString()));
                        currentValue.setLength(0);
                    }
                    continue;
                }

                for (final Token token : NON_VALUE_TOKENS) {
                    if (expr.regionMatches(i, token.value, 0, token.value.length())) {
                        // terminate value token
                        if (!currentValue.isEmpty()) {
                            ret.add(new Token(TokenType.VALUE, currentValue.toString()));
                            currentValue.setLength(0);
                        }

                        ret.add(token);
                        i += token.value.length();
                        continue char_iteration;
                    }
                }

                currentValue.append(c);
                ++i;
            }

            // terminate value token
            if (!currentValue.isEmpty()) {
                ret.add(new Token(TokenType.VALUE, currentValue.toString()));
                currentValue.setLength(0);
            }

            ret.add(new Token(TokenType.EOF, ""));
            return ret;
        }

        public boolean evaluate() throws Exception {
            final boolean ret = this.parseOr();
            if (this.tokens.get(this.pos).type != TokenType.EOF) {
                throw new Exception("Unexpected token at end of expression: " + this.tokens.get(this.pos).value);
            }
            return ret;
        }

        private boolean parseOr() throws Exception {
            boolean left = this.parseAnd();

            while (this.tokens.get(this.pos).type == TokenType.OR) {
                this.pos++;
                // must evaluate and
                left = left | this.parseAnd();
            }
            return left;
        }

        private boolean parseAnd() throws Exception {
            boolean left = this.parseEquality();
            while (this.tokens.get(this.pos).type == TokenType.AND) {
                this.pos++;
                // must evaluate equality
                left = left & this.parseEquality();
            }
            return left;
        }

        private boolean parseEquality() throws Exception {
            if (this.tokens.get(this.pos).type == TokenType.LPAREN) {
                // evaluate value in parentheses
                ++this.pos;
                boolean val = this.parseOr();
                if (this.tokens.get(this.pos).type != TokenType.RPAREN) {
                    throw new Exception("Missing closing parenthesis");
                }
                ++this.pos;
                return val;
            }

            final Token leftToken = this.tokens.get(this.pos++);
            if (leftToken.type != TokenType.VALUE) {
                throw new Exception("Expected value, got: " + leftToken.type);
            }

            final TokenType operator = this.tokens.get(this.pos).type;
            if (operator == TokenType.EQ || operator == TokenType.NEQ) {
                ++this.pos;

                final Token rightToken = this.tokens.get(this.pos);
                ++this.pos;
                if (rightToken.type != TokenType.VALUE) {
                    throw new Exception("Expected value after operator, got: " + rightToken.type);
                }

                final boolean res = leftToken.value.equals(rightToken.value);

                return res ^ (operator == TokenType.NEQ);
            }

            throw new Exception("Expected == or !=, got: " + operator);
        }

        private static enum TokenType {
            AND,
            OR,
            EQ,
            NEQ,
            LPAREN,
            RPAREN,
            VALUE,
            EOF;
        }

        private static record Token(TokenType type, String value) {}
    }
}