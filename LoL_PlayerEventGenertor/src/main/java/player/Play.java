package player;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class Play implements Runnable {

    private final String boootstrap_Server;

    private String sessionRoomID;
    static OffsetDateTime createRoomDate;

    private String ipAddr;
    private String account;
    private String champion;
    static Random rand = new Random();
    private int durationSeconds;

    private final long MINIMUM_SLEEP_TIME = 50;
    private final long MAXIMUM_SLEEP_TIME = 60 * 300;
    private final String TOPIC_NAME = "gamelogs";
    private CountDownLatch latch;


    public Play(CountDownLatch latch, String sessionRoomID, OffsetDateTime createRoomDate,
        String ipAddr, String account, String champion, int durationSeconds,
        String bootstrap_Server) {
        this.latch = latch;
        this.sessionRoomID = sessionRoomID;
        this.createRoomDate = createRoomDate;
        this.ipAddr = ipAddr;
        this.account = account;
        this.champion = champion;
        this.durationSeconds = durationSeconds;
        this.rand = new Random();
        this.boootstrap_Server = bootstrap_Server;
    }

    @Override
    public void run() {
        System.out.println(
            account + "님이 참가하셨습니다. 챔피언: " + champion + " ( ip: " + ipAddr + ", account=" + account
                + ", sessionRoomID=" + sessionRoomID + ", durationSeconds=" + durationSeconds);

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boootstrap_Server);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "League_Of_Legend");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        long startTime = System.currentTimeMillis();
        Integer deathCount = 0;
        Integer itemCount = 0;

        while (isDuration(startTime)) {
            long sleepTime =
                MINIMUM_SLEEP_TIME + Double.valueOf(rand.nextDouble() * (MAXIMUM_SLEEP_TIME))
                    .longValue();

            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String method = getMethod();
            OffsetDateTime offsetDateTime = OffsetDateTime.now(ZoneId.of("UTC"));

            long endTime = System.currentTimeMillis();
            long runTime_seconds = (endTime - startTime) / 1000;
            long runTime_min = runTime_seconds / 60;

            String finalTime = runTime_min + ":" + (runTime_seconds % 60);
            Integer x_direction = getMouseX();
            Integer y_direction = getMouseY();
            String key = getPlayerInputKey();
            Integer status = getStatus();

            if (champion.equals("viktor")) {
                x_direction = getMouseX_Viktor();
                y_direction = getMouseY_Viktor();
                printPlayerLog(sessionRoomID, createRoomDate, ipAddr, account, champion, method,
                    offsetDateTime, x_direction, y_direction, key, status, deathCount, finalTime,
                    producer);
            } else if (method.equals("/wait")) {
                printPlayerLog(sessionRoomID, createRoomDate, ipAddr, account, champion, method,
                    offsetDateTime, 0, 0, "0", status, deathCount, finalTime,
                    producer);
            } else if (rand.nextDouble() > 0.97 && itemCount < 6) {
                itemCount += 1;
                printPlayerLog(sessionRoomID, createRoomDate, ipAddr, account, champion, "/buyItem",
                    offsetDateTime, x_direction, y_direction, key, status, deathCount, finalTime,
                    producer);
            } else if (status == 1) {
                printPlayerLog(sessionRoomID, createRoomDate, ipAddr, account, champion, method,
                    offsetDateTime, x_direction, y_direction, key, status, deathCount, finalTime,
                    producer);
                status = 0;
                deathCount += 1;
            } else {
                printPlayerLog(sessionRoomID, createRoomDate, ipAddr, account, champion, method,
                    offsetDateTime, x_direction, y_direction, key, status, deathCount, finalTime,
                    producer);
            }
        }
        System.out.println(
            "Stopping log generator (ipAddr=" + ipAddr + ", account=" + account + ", sessionID="
                + sessionRoomID + ", champion=" + champion + ", durationSeconds="
                + durationSeconds);

        this.latch.countDown();
    }

    /**
     * 플레이어의 이벤트를 생성하는 메서드입니다.
     *
     * @param sessionRoomID  RoomID
     * @param createRoomDate Room 생성시간
     * @param ipAddr         플레이어의 IP
     * @param account        플레이어의 계정명
     * @param champion       플레이어의 챔피언(=캐릭터)
     * @param method         플레이어가 발생시킨 메서드
     * @param offsetDateTime 로그가 생성된 시간
     * @param x_direction    마우스 X값
     * @param y_direction    마우스 Y값
     * @param key            플레이어가 입력한 키
     * @param status         플레이어의 상태
     * @param deathCount     플레이어의 죽은 횟수
     * @param finalTime      // Room이 진행 된 현재 시간
     * @param producer
     */
    private void printPlayerLog(String sessionRoomID, OffsetDateTime createRoomDate, String ipAddr,
        String account, String champion, String method, OffsetDateTime offsetDateTime,
        Integer x_direction, Integer y_direction, String key, Integer status, Integer deathCount,
        String finalTime, KafkaProducer<String, String> producer) {
        String log = String.format(
            "{" +
                "\"roomID\": \"%s\"," +
                "\"createRoomDate\": \"%s\"," +
                "\"ip\": \"%s\"," +
                "\"account\": \"%s\"," +
                "\"champion\": \"%s\"," +
                "\"method\": \"%s\"," +
                "\"datetime\": \"%s\"," +
                "\"x\": \"%s\"," +
                "\"y\": \"%s\"," +
                "\"inputkey\": \"%s\"," +
                "\"status\": \"%s\"," +
                "\"deathCount\": \"%s\"," +
                "\"ingametime\": \"%s\"" + "}", sessionRoomID, createRoomDate, ipAddr, account,
            champion, method, offsetDateTime, x_direction, y_direction, key, status, deathCount,
            finalTime
        );

        JSONParser jsonParser = new JSONParser();
        try {
            JSONObject jsonObject = (JSONObject) jsonParser.parse(log);
            String jsonLog = String.valueOf(jsonObject);
            producer.send(new ProducerRecord<>(TOPIC_NAME, jsonLog));
            System.out.println(jsonLog);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 주어진 시간으로부터 경과하지 않았는지 확인하는 메서드입니다.
     *
     * @param startTime 시작 시간
     * @return 현재 시간이 설정된 기간 내에 있으면 true, 그렇지 않으면 false를 반환합니다.
     */
    private boolean isDuration(long startTime) {
        return System.currentTimeMillis() - startTime < durationSeconds * 1000L;
    }

    /**
     * 플레이어가 발생시킨 메서드를 출력하는 메서드입니다.
     *
     * @return 15%의 확률로 /wait을 반환하고 85%의 확률로 /move를 반환합니다
     */
    private String getMethod() {
        if (rand.nextDouble() > 0.85) {
            return "/wait";
        } else {
            return "/move";
        }
    }

    /**
     * 플레이어의 마우스 X 좌표값을 생성히는 메서드입니다.
     *
     * @return 플레이어는 99%로 -400 ~ 400의 X값을 반환하고 1%로 -1250 ~ 1250의 X값을 반환합니다.
     */
    private int getMouseX() {
        int x = 0;

        if (rand.nextDouble() > 0.99) {
            x = rand.nextInt(1250 - (-1250) + 1) + (-1250);
            return x;
        } else {
            x = rand.nextInt(400 - (-400) + 1) + (-400);
            return x;
        }
    }

    /**
     * 플레이어의 마우스 Y 좌표값을 생성히는 메서드입니다.
     *
     * @return 플레이어는 99%로 -400 ~ 400의 Y값을 반환하고 1%로 -1250 ~ 1250의 X값을 반환합니다.
     */
    private int getMouseY() {
        int y = 0;

        if (rand.nextDouble() > 0.99) {
            y = rand.nextInt(1250 - (-1250) + 1) + (-1250);
            return y;
        } else {
            y = rand.nextInt(400 - (-400) + 1) + (-400);
            return y;
        }
    }

    /**
     * 플레이어의 마우스 X 좌표값을 생성히는 메서드입니다.
     * <p>
     * 이 메서드는 챔피언이 victor일 경우 발생하는 메서드입니다.
     *
     * @return 플레이어는 99%로 -400 ~ 400의 X값을 반환하고 30%로 -3000 ~ 3000의 X값을 반환합니다.
     */
    private int getMouseX_Viktor() {
        int x = 0;

        if (rand.nextDouble() > 0.70) {
            x = rand.nextInt(3000 - (-3000) + 1) + (-3000);
            return x;
        } else {
            x = rand.nextInt(400 - (-400) + 1) + (-400);
            return x;
        }
    }

    /**
     * 플레이어의 마우스 Y 좌표값을 생성히는 메서드입니다.
     * <p>
     * 이 메서드는 챔피언이 victor일 경우 발생하는 메서드입니다.
     *
     * @return 플레이어는 99%로 -400 ~ 400의 Y값을 반환하고 30%로 -3000 ~ 3000의 X값을 반환합니다.
     */
    private int getMouseY_Viktor() {
        int y = 0;

        if (rand.nextDouble() > 0.70) {
            y = rand.nextInt(3000 - (-3000) + 1) + (-3000);
            return y;
        } else {
            y = rand.nextInt(400 - (-400) + 1) + (-400);
            return y;
        }
    }

    /**
     * 사용자가 입력한 키를 생성하는 메서드입니다.
     *
     * @return 5%로 "alt", "tab", "alt+tab", "esc", "shift", "enter"를 반환하고 25%로 d f null 그리고 그 외엔 q w
     * e r space b 를 반환합니다
     */
    private String getPlayerInputKey() {
        if (rand.nextDouble() > 0.95) {
            String[] arrKey = new String[]{"alt", "tab", "alt+tab", "esc", "shift", "enter"};
            int num = rand.nextInt(arrKey.length);
            String key = arrKey[num];

            return key;
        } else if (rand.nextDouble() > 0.75) {
            String[] arrKey = new String[]{"d", "f", "null"};
            int num = rand.nextInt(arrKey.length);
            String key = arrKey[num];

            return key;
        } else {
            String[] arrKey = new String[]{"q", "w", "e", "r", "space", "b"};
            int num = rand.nextInt(arrKey.length);
            String key = arrKey[num];

            return key;
        }
    }

    /**
     * 사용자의 상태를 생성하는 메서드입니다. 상태는 플레이어의 생존 여부를 의미하고, status = 0일 경우 사망 status = 1일 경우 생존
     *
     * @return 5%로 1을 반환하고, 그 외는 0을 반환합니다.
     */
    private int getStatus() {
        if (rand.nextDouble() > 0.95) {
            return 1;
        } else {
            return 0;
        }
    }
}
