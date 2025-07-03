package de.marvinxmo.versys.dsm.core;

/**
 * Enumeration representing the three possible combinations according to the CAP theorem
 */
public enum CAPType {
    AP("Availability + Partition Tolerance", 
       "High availability, eventual consistency, continues during network partitions"),
    
    CA("Consistency + Availability", 
       "Strong consistency, immediate availability, assumes no network partitions"),
    
    CP("Consistency + Partition Tolerance", 
       "Strong consistency, partition tolerant, may sacrifice availability during partitions");

    private final String fullName;
    private final String description;

    CAPType(String fullName, String description) {
        this.fullName = fullName;
        this.description = description;
    }

    public String getFullName() {
        return fullName;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return String.format("%s (%s)", name(), fullName);
    }
}
