package player;

import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class Room implements Runnable {

    static int userNum;
    static int player = 10;
    private final String sessionRoomID;
    static OffsetDateTime createRoomDate;

    static Set<String> ipSet = new HashSet<>();
    static Set<String> accSet = new HashSet<>();
    static Set<Integer> championSet = new HashSet<>();
    static Random rand = new Random();

    public Room(String sessionRoomId, OffsetDateTime createRoomDate, int userNum) {
        this.sessionRoomID = sessionRoomId;
        this.createRoomDate = createRoomDate;
        this.userNum = userNum;
    }

    CountDownLatch latch = new CountDownLatch(player);
    ExecutorService executor = Executors.newFixedThreadPool(player);

    @Override
    public void run() {
        int durationSeconds = rand.nextInt(3600 - (1200) + 1) + (1200);

        System.out.println(
            "방을 생성되었습니다. sessionRoomID=" + sessionRoomID + ", createRoomDate=" + createRoomDate
                + ", durationSeconds=" + durationSeconds);

        IntStream.range(0, player).forEach(j -> {
            String ipAddr = getIpAddr();
            String account = getAccount();
            String champion = getChampions();

            executor.execute(
                new Play(latch, sessionRoomID, createRoomDate, ipAddr, account, champion,
                    durationSeconds));

            championSet.clear();
        });

        executor.shutdown();

        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println(e);
        }
    }

    /**
     * 챔피언을 지정합니다.
     * <p>
     * 이 메서드는 챔피언을 생성하고, 중복을 허용하지 않습니다.
     *
     * @return Set<String> championSet에 생성된 챔피언이 없을 경우 반환하고, 있을 시 반환하지 않습니다.
     */
    private static String getChampions() {
        while (true) {
            int championStatus = rand.nextInt(15);
            String championName = "";
            if (!championSet.contains(championStatus)) {
                championSet.add(championStatus);

                if (championStatus == 0) {
                    championName = "azir";
                } else if (championStatus == 1) {
                    championName = "viktor";
                } else if (championStatus == 2) {
                    championName = "orianna";
                } else if (championStatus == 3) {
                    championName = "vex";
                } else if (championStatus == 4) {
                    championName = "ryze";
                } else if (championStatus == 5) {
                    championName = "ari";
                } else if (championStatus == 6) {
                    championName = "syndra";
                } else if (championStatus == 7) {
                    championName = "taliyah";
                } else if (championStatus == 8) {
                    championName = "xerath";
                } else if (championStatus == 9) {
                    championName = "malzahar";
                } else if (championStatus == 10) {
                    championName = "anivia";
                } else if (championStatus == 11) {
                    championName = "sylas";
                } else if (championStatus == 12) {
                    championName = "aurelion sol";
                } else if (championStatus == 13) {
                    championName = "rux";
                } else if (championStatus == 14) {
                    championName = "vladimir";
                } else if (championStatus == 15) {
                    championName = "neeko";
                } else {
                    championName = "zilean";
                }
                return championName;
            }
        }
    }

    /**
     * 새로운 IP를 생성합니다.
     * <p>
     * 이 메서드는 플레이어의 IP 주소를 생성하고, 중복을 허용하지 않습니다.
     *
     * @return Set<String> ipSet에 생성된 IP가 없을 시 반환하고, 있을 시 반환하지 않습니다.
     */
    private static String getIpAddr() {
        while (true) {
            String ipAddr = "192.168.0." + rand.nextInt(256);

            if (!ipSet.contains(ipAddr)) {
                ipSet.add(ipAddr);

                return ipAddr;
            }
        }
    }

    /**
     * 새로운 계정을 생성합니다.
     * <p>
     * 이 메서드는 플레이어의 수의 맞게 계정을 생성합니다. 중복된 계정은 생성하지 않습니다.
     *
     * @return Set<String> accSet에 해당 account가 없을 시 반환하고, 있을 시 반환하지 않습니다.
     */
    private static String getAccount() {

        while (true) {
            String account = "testAccount_" + rand.nextInt(userNum);

            if (!accSet.contains(account)) {
                accSet.add(account);

                return account;
            }
        }
    }

}
