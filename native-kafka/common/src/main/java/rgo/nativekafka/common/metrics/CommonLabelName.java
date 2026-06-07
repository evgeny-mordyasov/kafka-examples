package rgo.nativekafka.common.metrics;

public enum CommonLabelName {
    STATUS("status"),
    METHOD("method"),
    DIRECTION("direction"),
    TYPE("type"),
    ID("id"),
    ;

    private final String lowerCaseName;

    CommonLabelName(String lowerCaseName) {
        this.lowerCaseName = lowerCaseName;
    }

    public String lowerCaseName() {
        return lowerCaseName;
    }
}
