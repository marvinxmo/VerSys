package de.marvinxmo.versys;

import java.util.ArrayList;
import java.util.List;

public class ConsistentRingToRuleThemAll {

    final int ringSize = 20;
    final float init_p_fire = 0.8f;
    final int max_consecutive_misfires = 10;
    final int waitTime = 15000;

    class Coordinator {

        // private final int waitTime;
        private final NetworkConnection nc = new NetworkConnection("Coordinator");
        int max_consecutive_misfires;
        int waitTime;

        public Coordinator(int waitTime, int max_consecutive_misfires) {
            this.waitTime = waitTime;
            this.max_consecutive_misfires = max_consecutive_misfires;
        }

        public void engage() {
            nc.engage(this::start);
        }

        private void start() {
            Message m = new Message();

            // Payload (ring configuration)
            m.add("token", "continue");
            m.add("max_consecutive_misfires", max_consecutive_misfires);
            m.add("consecutive_misfires", 0);

            // Stats
            m.add("total_forwards", 0);
            m.add("total_fires", 0);
            m.add("total_misfires", 0);
            m.add("total_roundtrips", 0);
            List<Double> roundtrip_times = new ArrayList<Double>();
            m.add("roundtrip_times", roundtrip_times);
            m.add("p_fail", 0f);

            nc.sendBlindly(m, "0");

            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            m = new Message().add("token", "end");
            m.addHeader("sender", "Coordinator");
            nc.send(m); // this is a broadcast to all nodes
        }

    }

    class RingSegment extends Node {
        public RingSegment(int id, int next_id, float p_fire) {
            super(String.valueOf(id));
            this.id = String.valueOf(id);
            this.next_id = String.valueOf(next_id);
            this.p_fire = p_fire;
        }

        private final String id;
        private final String next_id;
        long last_forward_time;
        float p_fire;

        // Stats about the ring simulation
        int total_forwards = 0;
        int total_fires = 0;
        int total_misfires = 0;
        int total_roundtrips = 0;
        List<Double> roundtrip_times = new ArrayList<Double>();
        float p_fail = 0f;
        int max_consecutive_misfires = 0;

        @Override
        public void engage() {

            Message m;
            while (true) {
                m = receive();

                String value = m.query("token");

                if ("fire".equals(value)) {

                    // System.out.printf("Node %s saw firework from Node %s.\n", NodeName(),
                    // m.queryHeader("sender"));
                    continue;
                }

                if (value.equals("end")) {

                    if ("Coordinator".equals(m.queryHeader("sender"))) {

                        sleep(Integer.parseInt(id) * 400);
                        System.out.printf(
                                "Node %s received end message from Coordinator; Reason: Timeout\n",
                                NodeName());
                        System.out.printf(
                                "Node %s's stats are not in sync.\n",
                                NodeName());
                        printStats();
                    } else {
                        System.out.printf(
                                "Node %s received end message from %s; Reason: max_consecutive_misfires reached.\n",
                                NodeName(),
                                m.queryHeader("sender"));
                        copyStatsToNode(m);
                    }

                    if (Integer.parseInt(id) == 0) {
                        printStats();
                    }

                    return;
                }

                if (value.equals("continue")) {
                    System.out.printf("Node %s received token from %s\n", NodeName(),
                            m.queryHeader("sender"));

                    m.add("total_forwards", m.queryInteger("total_forwards") + 1);
                    copyStatsToNode(m);

                    if (Integer.parseInt(id) == 0 && !"Coordinator".equals(m.queryHeader("sender"))) {

                        m.add("total_roundtrips", m.queryInteger("total_roundtrips") + 1);

                        long receive_time = System.nanoTime();
                        double round_time_ms = (receive_time - last_forward_time) / 1_000_000.0;

                        List<Double> roundtrip_times = m.queryDoubleArray("roundtrip_times");
                        roundtrip_times.add(round_time_ms);
                        m.add("roundtrip_times", roundtrip_times);

                    }
                }

                int max_consecutive_misfires = m.queryInteger("max_consecutive_misfires");
                int consecutive_misfires = m.queryInteger("consecutive_misfires");

                float r = (float) Math.random();

                if (r < p_fire) {
                    System.out.printf("Node %s fired (p_fire= %s)\n", NodeName(),
                            p_fire);

                    m.add("token", "continue");
                    m.add("consecutive_misfires", 0);
                    m.add("total_fires", m.queryInteger("total_fires") + 1);

                    Message fire_message = new Message();
                    fire_message.add("token", "fire");
                    fire_message.addHeader("sender", NodeName());
                    broadcast(fire_message);

                } else {
                    System.out.printf("Node %s misfired (p_fire= %s)\n", NodeName(),
                            p_fire);

                    m.add("total_misfires", m.queryInteger("total_misfires") + 1);
                    consecutive_misfires++;

                    if (consecutive_misfires < max_consecutive_misfires) {
                        m.add("token", "continue");
                        m.add("consecutive_misfires", consecutive_misfires);

                    } else {
                        m.add("token", "end");
                        m.add("p_fail", p_fire);
                        System.out.printf("Reached %s consecutive misfires!; initiating termination\n",
                                max_consecutive_misfires);

                        broadcast(m);
                        return;
                    }
                }
                p_fire = p_fire / 2;
                sleep(100);

                try {
                    send(m, next_id);
                } catch (UnknownNodeException e) {
                    System.out.println("Unkown Node Exception: " + e.getMessage());
                    System.out.printf("Most likely Node %s tried to contact a Node that is not the network: Node %s\n",
                            NodeName(), next_id);
                }

                last_forward_time = System.nanoTime();
            }
        }

        void copyStatsToNode(Message m) {
            // Copy stats from coordinator to node
            total_forwards = m.queryInteger("total_forwards");
            total_fires = m.queryInteger("total_fires");
            total_misfires = m.queryInteger("total_misfires");
            total_roundtrips = m.queryInteger("total_roundtrips");
            roundtrip_times = m.queryDoubleArray("roundtrip_times");
            p_fail = m.queryFloat("p_fail");
            max_consecutive_misfires = m.queryInteger("max_consecutive_misfires");

        }

        void printStats() {

            System.out.printf("------------Node %s Stats---------------", NodeName());
            System.out.println();
            System.out.printf("total_forward: %s\n", total_forwards);
            System.out.printf("total_fires: %s\n", total_fires);
            System.out.printf("total_misfires: %s\n", total_misfires);
            System.out.printf("total_roundtrips: %s\n", total_roundtrips);
            System.out.printf("roundtrip_times: %s\n", roundtrip_times);
            System.out.printf("average roundtrip time: %s\n",
                    roundtrip_times.stream().mapToDouble(Double::doubleValue)
                            .average().orElse(0.0));
            System.out.printf("min roundtrip time: %s\n",
                    roundtrip_times.stream().mapToDouble(Double::doubleValue)
                            .min().orElse(0.0));
            System.out.printf("max roundtrip time: %s\n",
                    roundtrip_times.stream().mapToDouble(Double::doubleValue)
                            .max().orElse(0.0));
            System.out.printf("p_fail: %s\n", p_fail);
            System.out.printf("max_consecutive_misfires: %s\n",
                    max_consecutive_misfires);
            System.out.printf("------------Node %s Stats End-----------", NodeName());
            System.out.println();
            System.out.println();

        }
    }

    void testConsistentRingToRuleThemAll() {
        final int ringSize = 20;
        Simulator simulator = Simulator.getInstance();
        RingSegment[] segments = new RingSegment[ringSize];

        float init_p_fire = 0.5f;

        for (int i = 0; i < ringSize; i++) {
            segments[i] = new RingSegment(i, (i + 1) % ringSize, init_p_fire);
        }
        // Terminate after 4 consecutive misfires or when time limit is reached.
        Coordinator coordinator = new Coordinator(3000, 6);
        coordinator.engage();

        simulator.simulate();
        simulator.shutdown();
    }

    public static void main(String[] args) {
        ConsistentRingToRuleThemAll ConsistentRingToRuleThemAll = new ConsistentRingToRuleThemAll();
        ConsistentRingToRuleThemAll.testConsistentRingToRuleThemAll();
    }
}