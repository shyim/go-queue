package queue_test

import (
	"context"
	"fmt"
	"time"

	"github.com/shyim/go-queue"
	"github.com/shyim/go-queue/transport/memory"
)

func Example() {
	type SendEmail struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
		Body    string `json:"body"`
	}

	type ResizeImage struct {
		URL    string `json:"url"`
		Width  int    `json:"width"`
		Height int    `json:"height"`
	}

	bus := queue.NewBus()
	transport := memory.NewTransport()
	bus.AddTransport("async", transport)

	queue.HandleFunc[SendEmail](bus, "async", func(ctx context.Context, msg SendEmail) error {
		fmt.Printf("Sending email to %s: %s\n", msg.To, msg.Subject)
		return nil
	})

	queue.HandleFunc[ResizeImage](bus, "async", func(ctx context.Context, msg ResizeImage) error {
		fmt.Printf("Resizing %s to %dx%d\n", msg.URL, msg.Width, msg.Height)
		return nil
	})

	ctx := context.Background()
	_ = queue.Dispatch(ctx, bus, SendEmail{
		To:      "user@example.com",
		Subject: "Welcome!",
		Body:    "Hello, world!",
	})

	_ = queue.Dispatch(ctx, bus, ResizeImage{
		URL:    "https://example.com/photo.jpg",
		Width:  800,
		Height: 600,
	}, queue.WithDelay(5*time.Second), queue.WithMaxRetries(5))

	_ = bus.Setup(ctx)

	workerCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	worker := queue.NewWorker(bus, queue.WorkerConfig{Concurrency: 1})
	_ = worker.Run(workerCtx)

	// Unordered output:
	// Sending email to user@example.com: Welcome!
	// Resizing https://example.com/photo.jpg to 800x600
}
