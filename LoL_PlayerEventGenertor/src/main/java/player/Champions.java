package player;

import java.util.Arrays;

public enum Champions {
    AZIR(0, "AZIR"),
    VIKTOR(1, "VIKTOR"),
    ORIANA(2, "ORIANA"),
    VEX(3, "VEX"),
    RYZE(4, "RYZE"),
    ARI(5, "ARI"),
    SYNDRA(6, "SYNDRA"),
    TAILIYAH(7, "TAILIYAH"),
    XERATH(8, "XERATH"),
    MALZAHAR(9, "MALZAHAR"),
    ANIVIA(10, "ANIVIA"),
    SYLAS(11, "SYLAS"),
    AURELION_SOL(12, "AURELION_SOL"),
    RUX(13, "RUX"),
    VLADIMIR(14, "VLADIMIR"),
    NEEKO(15, "NEEKO"),
    ZILEAN(16, "ZILEAN");

    private final int value;
    private final String name;


    Champions(int value, String name) {
        this.value = value;
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

}
