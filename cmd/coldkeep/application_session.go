package main

import (
	"context"
	"github.com/franchoy/coldkeep/internal/application"
	"github.com/franchoy/coldkeep/internal/engine"
)

type commandSession interface {
	Engine() engine.Engine
	OperationContext(context.Context) (context.Context, context.CancelFunc)
	Close() error
}

func openApplicationSession(req application.Request) (commandSession, error) {
	return application.Open(req)
}

var openApplicationSessionPhase = openApplicationSession

func openCommandSession(operation string, requireStorage bool, containerDir string) (commandSession, error) {
	return openApplicationSessionPhase(application.Request{
		Operation: operation, RequireStorage: requireStorage, ContainerDir: containerDir,
	})
}
