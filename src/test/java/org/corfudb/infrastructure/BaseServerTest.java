package org.corfudb.infrastructure;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/14/15.
 */
public class BaseServerTest extends AbstractServerTest {

    BaseServer server;

    @Before
    public void setupTest() {
        server = new BaseServer();
        this.setServer(server);
    }

    @Override
    public AbstractServer getDefaultServer() {
        return null;
    }

    @Test
    public void testPing() {
        sendMessage(new CorfuMsg(CorfuMsgType.PING));
        assertThat(getLastMessage().getMsgType())
                .isEqualTo(CorfuMsgType.PONG);
    }

    @Test
    public void shutdownServerDoesNotRespond() {
        server.shutdown();
        sendMessage(new CorfuMsg(CorfuMsgType.PING));
        assertThat(getLastMessage())
                .isNull();
    }
}
