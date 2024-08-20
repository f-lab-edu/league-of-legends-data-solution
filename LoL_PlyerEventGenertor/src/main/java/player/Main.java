package player;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class Main {
    static int userNum = 100 ; // 플레이어의 수를 지정합니다.
    static int roomNum = userNum/10; // 방의 수를 변수로 저장합니다.

    public static void main(String[] args) {
        CountDownLatch latch = new CountDownLatch(roomNum);
        ExecutorService executor = Executors.newFixedThreadPool(roomNum);

        IntStream.range(0,roomNum).forEach(j -> {
            String sessionRoomID = getSessionRoomID();
            OffsetDateTime createRoomDate = OffsetDateTime.now(ZoneId.of("UTC"));
            executor.execute(new Room(sessionRoomID, createRoomDate, userNum));
        });

        executor.shutdown();

        try {
            latch.await();
        }catch (InterruptedException e) {
            System.err.println(e);
        }
    }

    public static String getSessionRoomID() { // 생성된 Room의 ID를 생성하는 함수입니다.
        String sessionRoomID = String.valueOf(UUID.randomUUID());

        return sessionRoomID;
    }
}
