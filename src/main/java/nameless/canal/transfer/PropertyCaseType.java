package nameless.canal.transfer;

public enum PropertyCaseType {
    LOWER_CASE {
        public String normalizeKey(String key) {
            return key.equals(key.toUpperCase()) ? key.toLowerCase() : key;
        }
    },
    UPPER_CASE {
        public String normalizeKey(String key) {
            return key.equals(key.toLowerCase()) ? key.toUpperCase() : key;
        }
    },
    AS_IS {
        public String normalizeKey(String key) {
            return key;
        }
    };

    public String normalizeKey(String key)  {
        throw new AbstractMethodError();
    }
}
