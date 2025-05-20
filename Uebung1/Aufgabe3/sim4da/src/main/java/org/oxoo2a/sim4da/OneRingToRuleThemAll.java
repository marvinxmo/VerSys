package org.oxoo2a.sim4da;

public class OneRingToRuleThemAll {

    class Coordinator {

        private final int waitTime;
        private final NetworkConnection nc = new NetworkConnection("Coordinator");
        int max_consecutive_misfires;

        public Coordinator(int waitTime, int max_consecutive_misfires) {
            this.waitTime = waitTime;
            this.max_consecutive_misfires = max_consecutive_misfires;
        }

        public void engage() {
            nc.engage(this::start);
        }

        private void start() {
            Message m = new Message();

            m.add("token", "continue");
            m.add("max_consecutive_misfires", max_consecutive_misfires);
            m.add("consecutive_misfires", 0);

            nc.sendBlindly(m, "0");
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            m = new Message().add("token", "end");
            nc.sendBlindly(m, "0");
        }

    }

    class RingSegment extends Node {
        public RingSegment(int id, int next_id, float p_fire) {
            super(String.valueOf(id));
            this.id = String.valueOf(id);
            this.next_id = String.valueOf(next_id);
            this.p_fire = p_fire;
        }

        @SuppressWarnings("unused")
        private final String id;
        private final String next_id;
        float p_fire;

        @Override
        public void engage() {

            Message m;
            while (true) {
                m = receive();

                String value = m.query("token");

                if (value.equals("end")) {
                    System.out.printf("Ring segment %s received end message; terminating.\n",
                            NodeName());
                    sendBlindly(m, next_id);
                    break;
                }

                if (value.equals("continue")) {
                    System.out.printf("Ring segment %s received token from %s\n", NodeName(),
                            m.queryHeader("sender"));
                }

                int max_consecutive_misfires = m.queryInteger("max_consecutive_misfires");
                int consecutive_misfires = m.queryInteger("consecutive_misfires");

                float r = (float) Math.random();

                Message new_m = new Message();
                new_m.add("max_consecutive_misfires", max_consecutive_misfires);

                if (r < p_fire) {
                    System.out.printf("Ring segment %s fired (p_fire= %s)\n", NodeName(),
                            p_fire);

                    new_m.add("token", "continue");
                    new_m.add("consecutive_misfires", 0);

                } else {
                    System.out.printf("Ring segment %s misfired (p_fire= %s)\n", NodeName(),
                            p_fire);

                    consecutive_misfires++;

                    System.out.printf(
                            "consecutive misfires: %s; max: %s \n",
                            consecutive_misfires, max_consecutive_misfires);

                    if (consecutive_misfires < max_consecutive_misfires) {
                        new_m.add("token", "continue");
                        new_m.add("consecutive_misfires", consecutive_misfires);
                    } else {
                        new_m.add("token", "end");
                        new_m.add("consecutive_misfires", 0);
                        System.out.printf("Too many consecutive misfires!; initiating termination\n");
                    }
                }
                p_fire = p_fire / 2;
                sleep(100);
                sendBlindly(new_m, next_id);
            }
        }

    }

    void testOneRingToRuleThemAll() {
        final int ringSize = 20;
        Simulator simulator = Simulator.getInstance();
        RingSegment[] segments = new RingSegment[ringSize];

        float init_p_fire = 0.4f;

        for (int i = 0; i < ringSize; i++) {
            segments[i] = new RingSegment(i, (i + 1) % ringSize, init_p_fire);
        }
        // Terminate after 5000ms or 4 consecutive misfires, whatever occurs first
        Coordinator coordinator = new Coordinator(5000, 4);
        coordinator.engage();

        simulator.simulate();
        simulator.shutdown();
    }

    public static void main(String[] args) {
        OneRingToRuleThemAll oneRingToRuleThemAll = new OneRingToRuleThemAll();
        oneRingToRuleThemAll.testOneRingToRuleThemAll();
    }
}