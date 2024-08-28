package player;

import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.apache.kafka.common.protocol.types.Field.Str;

public class Room implements Runnable {

    final Integer PLAYER_LIMIT = 10;
    private final String IDENTIFIER;

    static int userNum;
    private final String sessionRoomID;
    private final String bootstrap_Server;
    static OffsetDateTime createRoomDate;

    static Set<String> ipSet = new HashSet<>();
    static Set<String> accSet = new HashSet<>();
    static Set<Integer> championSet = new HashSet<>();
    static Random rand = new Random();

    static CountDownLatch latch_main;

    public Room(String sessionRoomId,
        OffsetDateTime createRoomDate, int userNum,
        CountDownLatch latch_main, String bootstrap_Server, String IDENTIFIER) {
        this.sessionRoomID = sessionRoomId;
        this.createRoomDate = createRoomDate;
        this.userNum = userNum;
        this.latch_main = latch_main;
        this.bootstrap_Server = bootstrap_Server;
        this.IDENTIFIER = IDENTIFIER;
    }

    CountDownLatch latch = new CountDownLatch(PLAYER_LIMIT);
    ExecutorService executor = Executors.newFixedThreadPool(PLAYER_LIMIT);

    @Override
    public void run() {
        int durationSeconds = rand.nextInt(3600 - (1200) + 1) + (1200);

        System.out.println(
            "방을 생성되었습니다. sessionRoomID=" + sessionRoomID + ", createRoomDate=" + createRoomDate
                + ", durationSeconds=" + durationSeconds);

        IntStream.range(0, PLAYER_LIMIT).forEach(j -> {
            String ipAddr = getIpAddr();
            String account = getAccount();
            String champion = getChampions();

            executor.execute(
                new Play(latch, sessionRoomID, createRoomDate, ipAddr, account, champion,
                    durationSeconds, bootstrap_Server, IDENTIFIER));

            championSet.clear();
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println(e);
        } finally {
            executor.shutdown();
            this.latch_main.countDown();
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
            int championStatus = rand.nextInt(Champions.values().length);

            for (Champions champion : Champions.values()) {
                if (champion.getValue() == championStatus && !championSet.contains(
                    championStatus)) {
                    championSet.add(championStatus);
                    return champion.getName();
                }
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
