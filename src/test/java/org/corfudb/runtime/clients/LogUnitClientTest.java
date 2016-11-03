package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 12/14/15.
 */
public class LogUnitClientTest extends AbstractClientTest {

    LogUnitClient client;
    ServerContext serverContext;

    @Override
    Set<AbstractServer> getServersForTest() {
        String dirPath = getTempDir();
        serverContext = new ServerContextBuilder()
                .setInitialToken(0)
                .setSingle(false)
                .setVerify(true)
                .setMemory(false)
                .setLogPath(dirPath)
                .setMaxCache(256000000)
                .setSync(false)
                .setServerRouter(serverRouter)
                .build();
        return new ImmutableSet.Builder<AbstractServer>()
                .add(new LogUnitServer(serverContext))
                .build();
    }

    @Override
    Set<IClient> getClientsForTest() {
        client = new LogUnitClient();
        return new ImmutableSet.Builder<IClient>()
                .add(new BaseClient())
                .add(client)
                .build();
    }

    @Test
    public void canReadWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
    }

    @Test
    public void overwriteThrowsException()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        assertThatThrownBy(() -> client.write(0, Collections.<UUID>emptySet(), 0,
                testString, Collections.emptyMap()).get())
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotBeOverwritten()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.fillHole(0).get();
        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.HOLE);

        assertThatThrownBy(() -> client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get())
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void holeFillCannotOverwrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();

        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);

        assertThatThrownBy(() -> client.fillHole(0).get())
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(OverwriteException.class);
    }

    @Test
    public void backpointersCanBeWrittenAndRead()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString,
                ImmutableMap.<UUID, Long>builder()
                        .put(CorfuRuntime.getStreamID("hello"), 1337L)
                        .put(CorfuRuntime.getStreamID("hello2"), 1338L)
                        .build()).get();

        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello"), 1337L);
        assertThat(r.getBackpointerMap())
                .containsEntry(CorfuRuntime.getStreamID("hello2"), 1338L);
    }

    @Test
    public void canCommitWrite()
            throws Exception {
        byte[] testString = "hello world".getBytes();
        client.write(0, Collections.<UUID>emptySet(), 0, testString, Collections.emptyMap()).get();
        client.writeCommit(null, 0, true).get();
        LogData r = client.read(0).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
        assertThat(r.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT));

        UUID streamA = CorfuRuntime.getStreamID("streamA");
        // Create log directories for streams
        String streamLogDir = (String) serverContext.getServerConfig().get("--log-path");
        streamLogDir = streamLogDir + File.separator + "log";
        File streamDir = new File(streamLogDir);
        assertThat(streamDir.mkdir()).isTrue();

        client.writeStream(1, Collections.singletonMap(streamA, 0L), testString).get();
        client.writeCommit(Collections.singletonMap(streamA, 0L), 10L, true).get(); // 10L shouldn't matter

        r = client.read(streamA, Range.singleton(0L)).get().getReadSet().get(0L);
        assertThat(r.getType())
                .isEqualTo(DataType.DATA);
        assertThat(r.getPayload(new CorfuRuntime()))
                .isEqualTo(testString);
        assertThat(r.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT));
    }
}
