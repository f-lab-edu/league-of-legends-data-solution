package player;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;


public class Main {

    static final Integer PLAYER_LIMIT = 10;
    static final String IDENTIFIER = getToday();

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("사용법: java Main <플레이어 수> <bootstrap 서버:포트>");
            System.exit(0);
        }

        int userNum = Integer.parseInt(args[0]);
        String bootstrap_Server = args[1];

        if (userNum % PLAYER_LIMIT != 0) { // 5 % 10 =
            System.out.println("플레이어의 수를 다시 입력 해 주세요.");
            System.exit(0);
        }

        int roomNum = userNum / PLAYER_LIMIT;

        CountDownLatch latch = new CountDownLatch(roomNum);
        ExecutorService executor = Executors.newFixedThreadPool(roomNum);

        IntStream.range(0, roomNum).forEach(j -> {
            String sessionRoomID = getSessionRoomID();
            OffsetDateTime createRoomDate = OffsetDateTime.now(ZoneId.of("UTC"));
            executor.execute(
                new Room(sessionRoomID, createRoomDate, userNum, latch, bootstrap_Server,
                    IDENTIFIER));
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println(e);
        } finally {
            executor.shutdown();
        }
    }


    /**
     * 새로운 UUID를 생성하여 String으로 변환하고, 이를 sessionRoomID 변수로 저장합니다.
     * <p>
     * 이 메서드는 생성된 Room을 고유하게 식별하기 위해 사용됩니다.
     *
     * @return 생성된 Room을 식별하는 고유한 ID인 sessionRoomID를 반환합니다.
     */
    public static String getSessionRoomID() {
        String sessionRoomID = String.valueOf(UUID.randomUUID());

        return sessionRoomID;
    }

    /**
     * Main을 실행 할 시 UTC 기준의 현재 시간을 출력하는 메서드입니다.
     *
     * @return yyyyMMdd의 포맷형식을 가진 String형 formatedNow를 반환합니다.
     */
    private static String getToday() {
        LocalDate now = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        String formatedNow = now.format(formatter);

        return formatedNow;
    }
}
