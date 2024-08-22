package com.example.spring.netty.spring_netty.server.clickhouse.http.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class OutboundExceptionHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx,
                                Throwable cause) {
        byte[] bytes = cause.getMessage().getBytes();
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, buf);
        response.headers().set(CONTENT_TYPE, "text/html; charset=UTF-8");
        response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
        response.headers().set(CONNECTION, CLOSE);

        ctx.channel().writeAndFlush(response);
        ChannelFuture f = ctx.write(response);
        f.addListener(ChannelFutureListener.CLOSE);
        cause.printStackTrace();
//        ctx.close();
    }
}
