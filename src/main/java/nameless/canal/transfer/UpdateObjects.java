package nameless.canal.transfer;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

@Getter
public class UpdateObjects {
    private List<UpdateObject> inserts = new ArrayList<>();
    private List<UpdateObject> updates = new ArrayList<>();
    private LinkedHashSet<UpdateObject> deletes = new LinkedHashSet<>();

    public void delete(String indexName, String id) {
        delete(new UpdateObject(indexName, id));
    }

    public void delete(UpdateObject delete) {
        deletes.add(delete);
        inserts.remove(delete);
        updates.remove(delete);
    }

    public void insert(UpdateObject insert) {
        inserts.add(insert);
        deletes.remove(insert);
    }

    public void update(UpdateObject update) {
        for (UpdateObject i : inserts) {
            if (i.equals(update)) {
                i.putAll(update);
                return;
            }
        }

        for (UpdateObject i : updates) {
            if (i.equals(update)) {
                i.putAll(update);
                return;
            }
        }

        updates.add(update);
    }

    public boolean exists(UpdateObject o) {
        return isInInsertBuffer(o) || isInUpdateBuffer(o);
    }

    public boolean isInInsertBuffer(UpdateObject o) {
        return inserts.contains(o);
    }

    public boolean isInUpdateBuffer(UpdateObject o) {
        return updates.contains(o);
    }

    public UpdateObject find(String indexName, String id) {
        return find(indexName, id, inserts, updates, deletes);

    }

    private UpdateObject find(String indexName, String id, Collection<UpdateObject>... collections) {
        for (Collection<UpdateObject> collection : collections) {
            for (UpdateObject i : collection) {
                if (i.getIndexName().equals(indexName) && i.getId().equals(id)) {
                    return i;
                }
            }
        }
        return null;
    }

}
