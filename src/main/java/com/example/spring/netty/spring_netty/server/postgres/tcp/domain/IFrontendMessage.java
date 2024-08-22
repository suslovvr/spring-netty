package com.example.spring.netty.spring_netty.server.postgres.tcp.domain;

public sealed interface IFrontendMessage permits FrontendCommandMessage, FrontendBootstrapMessage {
}
