package rpc

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func (s *Server) ServeKubeMQRequests(host string, port int, clientId string, channel string, group string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId(clientId),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	errCh := make(chan error)
	commandsCh, err := client.SubscribeToCommands(ctx, channel, group, errCh)
	if err != nil {
		log.Fatal(err)
	}
	for {
		select {
		case err := <-errCh:
			log.Fatal(err)
			return
		case command, more := <-commandsCh:
			if !more {
				log.Println("Command Received , done")
				return
			}
			log.Printf("Command Received:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", command.Id, command.Channel, command.Metadata, command.Body)
			err := client.R().
				SetRequestId(command.Id).
				SetResponseTo(command.ResponseTo).
				SetExecutedAt(time.Now()).
				Send(ctx)
			if err != nil {
				log.Fatal(err)
			}
		case <-ctx.Done():
			return
		}
	}
}
