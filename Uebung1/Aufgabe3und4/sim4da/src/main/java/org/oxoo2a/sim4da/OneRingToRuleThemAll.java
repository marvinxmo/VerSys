package org.oxoo2a.sim4da;

import java.util.ArrayList;
import java.util.List;

public class OneRingToRuleThemAll {

    class Coordinator {

        // private final int waitTime;
        private final NetworkConnection nc = new NetworkConnection("Coordinator");
        int max_consecutive_misfires;

        public Coordinator(int max_consecutive_misfires) {
            // this.waitTime = waitTime;
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

            // Time Limit causes issues with current implementation
            // The Ring only stops when the consecutive misfires exceed the limit

            // try {
            // Thread.sleep(waitTime);
            // } catch (InterruptedException e) {
            // e.printStackTrace();
            // }
            // m = new Message().add("token", "end");
            // nc.sendBlindly(m, "0");
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
        double last_forward_time;
        float p_fire;

        @Override
        public void engage() {

            Message m;
            while (true) {
                m = receive();

                String value = m.query("token");

                if (value == "fire") {
                    // Commented out to avoid longer round times caused by printingto console.
                    // System.out.printf("Node %s saw firework from Node %s.\n", NodeName(),
                    // m.queryHeader("sender"));
                    continue;
                }

                if (value.equals("end")) {
                    System.out.printf("Node %s received end message; terminating.\n",
                            NodeName());

                    if (Integer.parseInt(id) == 0) {

                        System.out.println("------------Stats---------------");
                        System.out.printf("total_forward: %s\n", m.queryInteger("total_forwards"));
                        System.out.printf("total_fires: %s\n", m.queryInteger("total_fires"));
                        System.out.printf("total_misfires: %s\n", m.queryInteger("total_misfires"));
                        System.out.printf("total_roundtrips: %s\n", m.queryInteger("total_roundtrips"));
                        System.out.printf("roundtrip_times: %s\n", m.queryDoubleArray("roundtrip_times"));

                        System.out.printf("average roundtrip time: %s\n",
                                m.queryDoubleArray("roundtrip_times").stream().mapToDouble(Double::doubleValue)
                                        .average().orElse(0.0));
                        System.out.printf("min roundtrip time: %s\n",
                                m.queryDoubleArray("roundtrip_times").stream().mapToDouble(Double::doubleValue)
                                        .min().orElse(0.0));
                        System.out.printf("max roundtrip time: %s\n",
                                m.queryDoubleArray("roundtrip_times").stream().mapToDouble(Double::doubleValue)
                                        .max().orElse(0.0));
                        System.out.printf("p_fail: %s\n", m.queryFloat("p_fail"));
                        System.out.printf("max_consecutive_misfires: %s\n",
                                m.queryInteger("max_consecutive_misfires"));
                        System.out.println("--------------Stats End---------------");

                    }

                    sendBlindly(m, next_id);
                    break;
                }

                if (value.equals("continue")) {
                    System.out.printf("Node %s received token from %s\n", NodeName(),
                            m.queryHeader("sender"));

                    m.add("total_forwards", m.queryInteger("total_forwards") + 1);

                    if (Integer.parseInt(id) == 0 && m.queryHeader("sender") != "Coordinator") {

                        m.add("total_roundtrips", m.queryInteger("total_roundtrips") + 1);

                        double receive_time = System.currentTimeMillis();
                        System.out.println("Received token at " + receive_time);
                        System.out.println("Last forward time: " + last_forward_time);
                        double round_time = receive_time - last_forward_time;
                        System.out.printf("Round trip time: %s\n", round_time);

                        List<Double> roundtrip_times = m.queryDoubleArray("roundtrip_times");
                        roundtrip_times.add(round_time);
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
                        m.add("consecutive_misfires", 0);
                        System.out.printf("Reached %s consecutive misfires!; initiating termination\n",
                                max_consecutive_misfires);

                        m.add("p_fail", p_fire);

                    }
                }
                p_fire = p_fire / 2;
                // sleep(100);
                sendBlindly(m, next_id);
                last_forward_time = System.currentTimeMillis();
            }
        }

    }

    void testOneRingToRuleThemAll() {
        final int ringSize = 20;
        Simulator simulator = Simulator.getInstance();
        RingSegment[] segments = new RingSegment[ringSize];

        float init_p_fire = 1f;

        for (int i = 0; i < ringSize; i++) {
            segments[i] = new RingSegment(i, (i + 1) % ringSize, init_p_fire);
        }
        // Terminate after 4 consecutive misfires (time limit not implemented)
        Coordinator coordinator = new Coordinator(25);
        coordinator.engage();

        simulator.simulate();
        simulator.shutdown();
    }

    public static void main(String[] args) {
        OneRingToRuleThemAll OneRingToRuleThemAll = new OneRingToRuleThemAll();
        OneRingToRuleThemAll.testOneRingToRuleThemAll();
    }
}