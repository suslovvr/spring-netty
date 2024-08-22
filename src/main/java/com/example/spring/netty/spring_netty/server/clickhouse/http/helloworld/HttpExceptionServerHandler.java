/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.example.spring.netty.spring_netty.server.clickhouse.http.helloworld;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import lombok.extern.slf4j.Slf4j;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.*;
import static io.netty.handler.codec.http.HttpResponseStatus.EXPECTATION_FAILED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
@Slf4j
public class HttpExceptionServerHandler extends SimpleChannelInboundHandler<HttpObject> {
    private static final byte[] CONTENT = { 'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd' };

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    static int  count=0;
    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {

            try {
                if(++count==1) {
//                    writeMessage(ctx, "no error..");
                    Exception e =new RuntimeException("parser error..");
//                    caughtException( ctx,  e);
                    throw e;
//                    return;
                }
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;

            boolean keepAlive = HttpUtil.isKeepAlive(req);
            FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), OK,
                    Unpooled.wrappedBuffer(CONTENT));
            response.headers()
                    .set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());

            if (keepAlive) {
                if (!req.protocolVersion().isKeepAliveDefault()) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }
            } else {
                // Tell the client we're going to close the connection.
                response.headers().set(CONNECTION, CLOSE);
            }

            ChannelFuture f = ctx.write(response);

            if (!keepAlive) {
                f.addListener(ChannelFutureListener.CLOSE);
            }
        }
            } catch (Exception e) {
                caughtException( ctx,  e);
                log.warn(e.getMessage());
//                ctx.fireExceptionCaught(e);
            }
    }
    public void writeMessage(ChannelHandlerContext ctx, String msg) {
        try {

            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, EXPECTATION_FAILED,
                    Unpooled.wrappedBuffer(msg.getBytes("UTF-8")));
            response.headers()
                    .set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());
            response.headers().set(CONNECTION, KEEP_ALIVE);


            // Tell the client we're going to close the connection.
//            response.headers().set(CONNECTION, CLOSE);

            ChannelFuture f = ctx.write(response);
//            f.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e) {
//            ctx.fireExceptionCaught(e);
        }
//        ctx.close();

    }
    public void caughtException(ChannelHandlerContext ctx, Throwable cause) {
        try {

            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, EXPECTATION_FAILED,
                    Unpooled.wrappedBuffer(cause.getMessage().getBytes("UTF-8")));
            response.headers()
                    .set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());
            response.headers().set(CONNECTION, KEEP_ALIVE);


            // Tell the client we're going to close the connection.
//            response.headers().set(CONNECTION, CLOSE);

            ChannelFuture f = ctx.write(response);
//            f.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e) {
//            ctx.fireExceptionCaught(e);
        }

        cause.printStackTrace();

    }
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        try {

        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, EXPECTATION_FAILED,
                Unpooled.wrappedBuffer(cause.getMessage().getBytes("UTF-8")));
        response.headers()
                .set(CONTENT_TYPE, TEXT_PLAIN)
                .setInt(CONTENT_LENGTH, response.content().readableBytes());


            // Tell the client we're going to close the connection.
            response.headers().set(CONNECTION, CLOSE);

        ChannelFuture f = ctx.write(response);
        f.addListener(ChannelFutureListener.CLOSE);
    } catch (Exception e) {
        ctx.fireExceptionCaught(e);
    }
        cause.printStackTrace();
        ctx.close();
    }
}
