// Example: Worker API usage with the Flo Go SDK
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	flo "github.com/floruntime/flo-go"
)

func main() {
	// Create a worker with configuration
	w, err := flo.NewWorker(flo.WorkerConfig{
		Endpoint:    getEnv("FLO_ENDPOINT", "localhost:3000"),
		Namespace:   getEnv("FLO_NAMESPACE", "myapp"),
		Concurrency: 10,
	})
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	defer w.Close()

	// Register action handlers
	w.MustRegisterAction("process-order", processOrder)
	w.MustRegisterAction("send-email", sendEmail)
	w.MustRegisterAction("validate-payment", validatePayment)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		w.Stop()
	}()

	// Start the worker (blocks until context is cancelled)
	log.Println("Worker starting...")
	if err := w.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Worker error: %v", err)
	}
}

// OrderRequest represents an incoming order
type OrderRequest struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Items      []Item  `json:"items"`
}

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// OrderResult represents the result of order processing
type OrderResult struct {
	OrderID   string `json:"order_id"`
	Status    string `json:"status"`
	ProcessedBy string `json:"processed_by"`
}

func processOrder(actx *flo.ActionContext) ([]byte, error) {
	// Parse input
	var req OrderRequest
	if err := actx.Into(&req); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	log.Printf("Processing order %s for customer %s (amount: $%.2f)",
		req.OrderID, req.CustomerID, req.Amount)

	// Simulate a long-running order processing task
	// For long tasks, periodically call Touch to extend the lease
	for i := 0; i < len(req.Items); i++ {
		item := req.Items[i]
		log.Printf("  Processing item %d/%d: %s (qty: %d)",
			i+1, len(req.Items), item.SKU, item.Quantity)

		// Simulate work for each item
		select {
		case <-actx.Ctx().Done():
			return nil, actx.Ctx().Err()
		case <-time.After(2 * time.Second):
			// Item processed
		}

		// Extend the lease every few items to prevent timeout
		// This is critical for long-running tasks
		if (i+1)%3 == 0 {
			if err := actx.Touch(30000); err != nil {
				log.Printf("Warning: failed to extend lease: %v", err)
			} else {
				log.Printf("  Extended lease for order %s", req.OrderID)
			}
		}
	}

	// Return result
	return actx.Bytes(OrderResult{
		OrderID:     req.OrderID,
		Status:      "processed",
		ProcessedBy: actx.TaskID(),
	})
}

// EmailRequest represents an email to send
type EmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func sendEmail(actx *flo.ActionContext) ([]byte, error) {
	var req EmailRequest
	if err := actx.Into(&req); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	log.Printf("Sending email to %s: %s", req.To, req.Subject)

	// Simulate email sending
	// In real code, this would use an email service

	return actx.Bytes(map[string]string{
		"status":  "sent",
		"to":      req.To,
		"subject": req.Subject,
	})
}

// PaymentRequest represents a payment to validate
type PaymentRequest struct {
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	CardLast4     string  `json:"card_last4"`
}

func validatePayment(actx *flo.ActionContext) ([]byte, error) {
	var req PaymentRequest
	if err := actx.Into(&req); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	log.Printf("Validating payment %s for $%.2f", req.TransactionID, req.Amount)

	// Check for context cancellation (timeout)
	select {
	case <-actx.Ctx().Done():
		return nil, actx.Ctx().Err()
	default:
	}

	// Simulate payment validation
	valid := req.Amount > 0 && req.Amount < 10000

	return actx.Bytes(map[string]interface{}{
		"transaction_id": req.TransactionID,
		"valid":          valid,
		"reason":         "",
	})
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
