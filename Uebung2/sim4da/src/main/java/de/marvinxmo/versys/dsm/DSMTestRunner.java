package de.marvinxmo.versys.dsm;

/**
 * Main class to run the DSM Test Suite
 * Simple entry point for testing all CAP theorem implementations
 */
public class DSMTestRunner {

    public static void main(String[] args) {
        System.out.println("🚀 Starting DSM Test Runner...");

        try {
            // Run the comprehensive test suite
            de.marvinxmo.versys.dsm.testing.DSMTestSuite.main(args);
        } catch (Exception e) {
            System.err.println("❌ Test runner failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
