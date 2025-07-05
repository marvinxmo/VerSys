package de.marvinxmo.versys.utils;

/**
 * Utility class for colored console output using ANSI escape codes.
 * Provides methods to print text in different colors and styles.
 * 
 * Usage:
 * ColorPrinter.printRed("This is red text");
 * ColorPrinter.printSuccess("Operation completed successfully");
 * ColorPrinter.printError("An error occurred");
 */
public class ColorPrinter {

    // ANSI Color codes
    private static final String RESET = "\u001B[0m";
    private static final String BLACK = "\u001B[30m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String PURPLE = "\u001B[35m";
    private static final String CYAN = "\u001B[36m";
    private static final String WHITE = "\u001B[37m";

    // Background colors
    private static final String BLACK_BG = "\u001B[40m";
    private static final String RED_BG = "\u001B[41m";
    private static final String GREEN_BG = "\u001B[42m";
    private static final String YELLOW_BG = "\u001B[43m";
    private static final String BLUE_BG = "\u001B[44m";
    private static final String PURPLE_BG = "\u001B[45m";
    private static final String CYAN_BG = "\u001B[46m";
    private static final String WHITE_BG = "\u001B[47m";

    // Text styles
    private static final String BOLD = "\u001B[1m";
    private static final String UNDERLINE = "\u001B[4m";
    private static final String ITALIC = "\u001B[3m";

    // Prevent instantiation
    private ColorPrinter() {
    }

    // === Basic Color Methods ===

    /**
     * Print text in black color
     */
    public static void printBlack(String message) {
        System.out.println(BLACK + message + RESET);
    }

    /**
     * Print text in red color
     */
    public static void printRed(String message) {
        System.out.println(RED + message + RESET);
    }

    /**
     * Print text in green color
     */
    public static void printGreen(String message) {
        System.out.println(GREEN + message + RESET);
    }

    /**
     * Print text in yellow color
     */
    public static void printYellow(String message) {
        System.out.println(YELLOW + message + RESET);
    }

    /**
     * Print text in blue color
     */
    public static void printBlue(String message) {
        System.out.println(BLUE + message + RESET);
    }

    /**
     * Print text in purple color
     */
    public static void printPurple(String message) {
        System.out.println(PURPLE + message + RESET);
    }

    /**
     * Print text in cyan color
     */
    public static void printCyan(String message) {
        System.out.println(CYAN + message + RESET);
    }

    /**
     * Print text in white color
     */
    public static void printWhite(String message) {
        System.out.println(WHITE + message + RESET);
    }

    // === Background Color Methods ===

    /**
     * Print text with black background
     */
    public static void printBlackBg(String message) {
        System.out.println(BLACK_BG + message + RESET);
    }

    /**
     * Print text with red background
     */
    public static void printRedBg(String message) {
        System.out.println(RED_BG + message + RESET);
    }

    /**
     * Print text with green background
     */
    public static void printGreenBg(String message) {
        System.out.println(GREEN_BG + message + RESET);
    }

    /**
     * Print text with yellow background
     */
    public static void printYellowBg(String message) {
        System.out.println(YELLOW_BG + message + RESET);
    }

    /**
     * Print text with blue background
     */
    public static void printBlueBg(String message) {
        System.out.println(BLUE_BG + message + RESET);
    }

    /**
     * Print text with purple background
     */
    public static void printPurpleBg(String message) {
        System.out.println(PURPLE_BG + message + RESET);
    }

    /**
     * Print text with cyan background
     */
    public static void printCyanBg(String message) {
        System.out.println(CYAN_BG + message + RESET);
    }

    /**
     * Print text with white background
     */
    public static void printWhiteBg(String message) {
        System.out.println(WHITE_BG + message + RESET);
    }

    // === Style Methods ===

    /**
     * Print text in bold
     */
    public static void printBold(String message) {
        System.out.println(BOLD + message + RESET);
    }

    /**
     * Print text with underline
     */
    public static void printUnderline(String message) {
        System.out.println(UNDERLINE + message + RESET);
    }

    /**
     * Print text in italic
     */
    public static void printItalic(String message) {
        System.out.println(ITALIC + message + RESET);
    }

    // === Combined Style Methods ===

    /**
     * Print text in bold red
     */
    public static void printBoldRed(String message) {
        System.out.println(BOLD + RED + message + RESET);
    }

    /**
     * Print text in bold green
     */
    public static void printBoldGreen(String message) {
        System.out.println(BOLD + GREEN + message + RESET);
    }

    /**
     * Print text in bold yellow
     */
    public static void printBoldYellow(String message) {
        System.out.println(BOLD + YELLOW + message + RESET);
    }

    /**
     * Print text in bold blue
     */
    public static void printBoldBlue(String message) {
        System.out.println(BOLD + BLUE + message + RESET);
    }

    // === Semantic Methods ===

    /**
     * Print success message (green with checkmark)
     */
    public static void printSuccess(String message) {
        System.out.println(GREEN + "✅ " + message + RESET);
    }

    /**
     * Print error message (red with X mark)
     */
    public static void printError(String message) {
        System.out.println(RED + "❌ " + message + RESET);
    }

    /**
     * Print warning message (yellow with warning sign)
     */
    public static void printWarning(String message) {
        System.out.println(YELLOW + "⚠️  " + message + RESET);
    }

    /**
     * Print info message (blue with info sign)
     */
    public static void printInfo(String message) {
        System.out.println(BLUE + "ℹ️  " + message + RESET);
    }

    /**
     * Print debug message (purple with debug sign)
     */
    public static void printDebug(String message) {
        System.out.println(PURPLE + "🐛 " + message + RESET);
    }

    /**
     * Print header with separator lines
     */
    public static void printHeader(String title) {
        String separator = "=".repeat(title.length() + 4);
        System.out.println(BOLD + BLUE + separator + RESET);
        System.out.println(BOLD + BLUE + "  " + title + "  " + RESET);
        System.out.println(BOLD + BLUE + separator + RESET);
    }

    /**
     * Print section separator
     */
    public static void printSeparator() {
        System.out.println(CYAN + "-".repeat(60) + RESET);
    }

    /**
     * Print formatted message with prefix
     */
    public static void printFormatted(String prefix, String message, String color) {
        String colorCode = getColorCode(color);
        System.out.printf(colorCode + "[%s] " + RESET + "%s%n", prefix, message);
    }

    /**
     * Helper method to get color code by name
     */
    private static String getColorCode(String color) {
        switch (color.toLowerCase()) {
            case "black":
                return BLACK;
            case "red":
                return RED;
            case "green":
                return GREEN;
            case "yellow":
                return YELLOW;
            case "blue":
                return BLUE;
            case "purple":
                return PURPLE;
            case "cyan":
                return CYAN;
            case "white":
                return WHITE;
            default:
                return RESET;
        }
    }

    // === Printf-style methods ===

    /**
     * Print formatted text in red
     */
    public static void printfRed(String format, Object... args) {
        System.out.printf(RED + format + RESET, args);
    }

    /**
     * Print formatted text in green
     */
    public static void printfGreen(String format, Object... args) {
        System.out.printf(GREEN + format + RESET, args);
    }

    /**
     * Print formatted text in yellow
     */
    public static void printfYellow(String format, Object... args) {
        System.out.printf(YELLOW + format + RESET, args);
    }

    /**
     * Print formatted text in blue
     */
    public static void printfBlue(String format, Object... args) {
        System.out.printf(BLUE + format + RESET, args);
    }

    /**
     * Print formatted text in purple
     */
    public static void printfPurple(String format, Object... args) {
        System.out.printf(PURPLE + format + RESET, args);
    }

    /**
     * Print formatted text in cyan
     */
    public static void printfCyan(String format, Object... args) {
        System.out.printf(CYAN + format + RESET, args);
    }
}
