package nameless.canal.transfer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class UpdateObject extends HashMap<String, Object> {

    private static final long serialVersionUID = 2168632330936708372L;

    private String indexName;

    private String id;

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private Set<String> unChangedColumns = new HashSet<>();

    public UpdateObject() {

    }

    public UpdateObject(String indexName, String id) {
        this.indexName = indexName;
        this.id = id;
    }

    public UpdateObject(String indexName, String id, Map<String, ?> fields) {
        this.indexName = indexName;
        this.id = id;
        setData(fields);
    }

    public void setData(Map<String, ?> values) {
        this.putAll(values);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Map))
            return false;
        return id.equals(((UpdateObject)o).id) && indexName.equals(((UpdateObject)o).indexName);
    }

    @Override
    public int hashCode() {
        return 31 * indexName.hashCode() + id.hashCode();
    }

    public Object put(String key, Object value, boolean changed) {
        if (!changed) {
            unChangedColumns.add(key);
        }
        return put(key, value);
    }

    @Setter(AccessLevel.NONE)
    @Getter(AccessLevel.NONE)
    private final Set<String> propertiesHaveSet = new HashSet<>();

    @Override
    public Object put(String key, Object value) {
        propertiesHaveSet.add(key);
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        propertiesHaveSet.addAll(m.keySet());
        super.putAll(m);
    }

    public boolean hasSetValue(String propertyName) {
        return propertiesHaveSet.contains(propertyName);
    }

    public UpdateObject changedOnlyObject() {
        UpdateObject o = new UpdateObject(indexName, id);
        forEach((key, value) -> {
            if (!unChangedColumns.contains(key)) {
                o.put(key, value);
            }
        });
        return o;
    }

    public boolean hasAnyChange() {
        return unChangedColumns.size() < size();
    }
}
