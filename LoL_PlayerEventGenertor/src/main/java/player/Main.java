package player;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class Main {

    /**
     * 로그를 생성하는 플레이어의 수를 저장합니다.
     */
    static int userNum = 10;
    /**
     * 생성되는 Room의 수를 저장합니다.
     */
    static int roomNum = userNum / 10;

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(roomNum);
        ExecutorService executor = Executors.newFixedThreadPool(roomNum);

        IntStream.range(0, roomNum).forEach(j -> {
            String sessionRoomID = getSessionRoomID();
            OffsetDateTime createRoomDate = OffsetDateTime.now(ZoneId.of("UTC"));
            executor.execute(new Room(sessionRoomID, createRoomDate, userNum));
        });

        executor.shutdown();

        try {
            latch.await();
        } catch (InterruptedException e) {
            System.err.println(e);
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
}
