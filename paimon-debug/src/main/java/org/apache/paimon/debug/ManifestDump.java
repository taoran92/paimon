/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.debug;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ManifestDump {

    private static final String WAREHOUSE = "file:///tmp/paimon-lab";
    private static final String DATABASE = "lab";
    private static final String TABLE = "t11_full_layout_pk";

    // Use null for latest snapshot, or set a concrete id such as 1L.
    private static final Long SNAPSHOT_ID = null;

    public static void main(String[] args) throws Exception {
        try (Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(WAREHOUSE))) {
            Table table = catalog.getTable(Identifier.create(DATABASE, TABLE));
            if (!(table instanceof FileStoreTable)) {
                throw new IllegalArgumentException("Not a FileStoreTable: " + TABLE);
            }

            FileStoreTable fileStoreTable = (FileStoreTable) table;
            Snapshot snapshot =
                    SNAPSHOT_ID != null
                            ? fileStoreTable.snapshotManager().snapshot(SNAPSHOT_ID)
                            : fileStoreTable.snapshotManager().latestSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("No snapshot found for " + TABLE);
            }

            ManifestList manifestList = fileStoreTable.store().manifestListFactory().create();
            ManifestFile manifestFile = fileStoreTable.store().manifestFileFactory().create();

            System.out.printf(
                    "Table: %s.%s%nLocation: %s%nSnapshot: %d%nSchemaId: %d%nCommitKind: %s%n%n",
                    DATABASE,
                    TABLE,
                    fileStoreTable.location(),
                    snapshot.id(),
                    snapshot.schemaId(),
                    snapshot.commitKind());

            dumpManifestList(
                    "baseManifestList",
                    snapshot.baseManifestList(),
                    snapshot.baseManifestListSize(),
                    manifestList,
                    manifestFile,
                    fileStoreTable);

            dumpManifestList(
                    "deltaManifestList",
                    snapshot.deltaManifestList(),
                    snapshot.deltaManifestListSize(),
                    manifestList,
                    manifestFile,
                    fileStoreTable);

            if (snapshot.changelogManifestList() != null) {
                dumpManifestList(
                        "changelogManifestList",
                        snapshot.changelogManifestList(),
                        snapshot.changelogManifestListSize(),
                        manifestList,
                        manifestFile,
                        fileStoreTable);
            }
        }
    }

    private static void dumpManifestList(
            String label,
            String manifestListName,
            Long manifestListSize,
            ManifestList manifestList,
            ManifestFile manifestFile,
            FileStoreTable table) {
        System.out.printf("== %s ==%n", label);
        System.out.printf("manifest-list file: %s, size: %s%n", manifestListName, manifestListSize);

        List<ManifestFileMeta> manifestMetas =
                manifestList.read(manifestListName, manifestListSize);
        if (manifestMetas.isEmpty()) {
            System.out.println("(empty)");
            System.out.println();
            return;
        }

        for (ManifestFileMeta meta : manifestMetas) {
            System.out.printf(
                    "  manifest file: %s, size=%d, added=%d, deleted=%d, schemaId=%d, "
                            + "bucket=[%s,%s], level=[%s,%s]%n",
                    meta.fileName(),
                    meta.fileSize(),
                    meta.numAddedFiles(),
                    meta.numDeletedFiles(),
                    meta.schemaId(),
                    meta.minBucket(),
                    meta.maxBucket(),
                    meta.minLevel(),
                    meta.maxLevel());

            List<ManifestEntry> entries = manifestFile.read(meta.fileName(), meta.fileSize());
            for (ManifestEntry entry : entries) {
                DataFileMeta file = entry.file();
                String partition = partitionString(table, entry);
                System.out.printf(
                        "    entry kind=%s partition=%s bucket=%d totalBuckets=%d%n",
                        entry.kind(), partition, entry.bucket(), entry.totalBuckets());
                System.out.printf(
                        "      file name=%s level=%d rows=%d bytes=%d schemaId=%d "
                                + "seq=[%d,%d] minKey=%s maxKey=%s%n",
                        file.fileName(),
                        file.level(),
                        file.rowCount(),
                        file.fileSize(),
                        file.schemaId(),
                        file.minSequenceNumber(),
                        file.maxSequenceNumber(),
                        file.minKey(),
                        file.maxKey());
            }
        }
        System.out.println();
    }

    private static String partitionString(FileStoreTable table, ManifestEntry entry) {
        LinkedHashMap<String, String> spec =
                table.store().partitionComputer().generatePartValues(entry.partition());
        if (spec.isEmpty()) {
            return "{}";
        }

        StringBuilder builder = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> part : spec.entrySet()) {
            if (!first) {
                builder.append(", ");
            }
            first = false;
            builder.append(part.getKey()).append("=").append(part.getValue());
        }
        return builder.append("}").toString();
    }
}
