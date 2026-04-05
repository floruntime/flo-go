// Example: ActionWorker with 13 action handlers for the Flo Go SDK
//
// This worker registers all handlers required by the workflow e2e test
// (process-order, expense-approval, signal-timeout-test, order-review).
//
// Usage:
//
//	cd sdks/go && go run ./examples/action_worker/
//
// Prerequisites:
//   - A running Flo server on localhost:4453
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	flo "github.com/floruntime/flo-go"
)

// =============================================================================
// Data Types
// =============================================================================

type OrderRequest struct {
	OrderID    string  `json:"orderId"`
	CustomerID string  `json:"customerId"`
	Amount     float64 `json:"amount"`
	Items      []Item  `json:"items"`
}

type Item struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

type NotificationRequest struct {
	UserID  string `json:"userId"`
	Channel string `json:"channel"`
	Message string `json:"message"`
}

// =============================================================================
// Action Handlers
// =============================================================================

// processOrder demonstrates long-running tasks with Touch.
func processOrder(actx *flo.ActionContext) (interface{}, error) {
	var req OrderRequest
	if err := actx.Into(&req); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	log.Printf("Processing order %s for customer %s (amount: $%.2f)",
		req.OrderID, req.CustomerID, req.Amount)

	for i, item := range req.Items {
		log.Printf("  Processing item %d/%d: %s (qty: %d)",
			i+1, len(req.Items), item.SKU, item.Quantity)

		time.Sleep(2 * time.Second)

		// Extend the lease every 3 items
		if (i+1)%3 == 0 {
			if err := actx.Touch(30000); err != nil {
				log.Printf("Warning: failed to extend lease: %v", err)
			} else {
				log.Printf("  Extended lease for order %s", req.OrderID)
			}
		}
	}

	return toBytes(map[string]interface{}{
		"orderId":     req.OrderID,
		"status":      "processed",
		"processedBy": actx.TaskID(),
	})
}

// sendNotification demonstrates returning a plain object (auto-marshaled).
func sendNotification(actx *flo.ActionContext) (interface{}, error) {
	var req NotificationRequest
	if err := actx.Into(&req); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	log.Printf("Sending %s notification to user %s: %s",
		req.Channel, req.UserID, req.Message)

	time.Sleep(500 * time.Millisecond)

	return map[string]interface{}{
		"success": true,
		"channel": req.Channel,
		"userId":  req.UserID,
	}, nil
}

// generateReport demonstrates context usage and lease extension.
func generateReport(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		Type      string `json:"type"`
		DateRange string `json:"dateRange"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	if data.Type == "" {
		data.Type = "summary"
	}
	if data.DateRange == "" {
		data.DateRange = "last_7_days"
	}

	log.Printf("Generating %s report for %s (attempt %d, task %s)",
		data.Type, data.DateRange, actx.Attempt(), actx.TaskID())

	totalSteps := 5
	for step := 0; step < totalSteps; step++ {
		log.Printf("  Report generation step %d/%d", step+1, totalSteps)
		time.Sleep(1 * time.Second)
		if step == 2 {
			_ = actx.Touch(30000)
		}
	}

	return toBytes(map[string]interface{}{
		"reportType":  data.Type,
		"dateRange":   data.DateRange,
		"generatedAt": fmt.Sprintf("%d", time.Now().UnixMilli()),
		"rows":        1500,
	})
}

// healthCheck — plain object return.
func healthCheck(actx *flo.ActionContext) (interface{}, error) {
	return map[string]interface{}{
		"status":    "healthy",
		"workerId":  actx.TaskID(),
		"timestamp": fmt.Sprintf("%d", time.Now().UnixMilli()),
	}, nil
}

// =============================================================================
// Workflow Action Handlers (used by workflow e2e)
// =============================================================================

// validateOrder rejects orders over $2000 or missing orderId.
func validateOrder(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string  `json:"orderId"`
		Amount  float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	log.Printf("[validate-order] Validating order %s ($%.2f)", data.OrderID, data.Amount)
	time.Sleep(200 * time.Millisecond)

	if data.OrderID == "" {
		log.Printf("[validate-order] Order is invalid: missing orderId")
		return nil, flo.NewNonRetryableErrorf("missing orderId")
	}
	if data.Amount > 2000 {
		log.Printf("[validate-order] Order %s is invalid: amount $%.2f exceeds limit", data.OrderID, data.Amount)
		return nil, flo.NewNonRetryableErrorf("order amount $%.0f exceeds $2000 limit", data.Amount)
	}
	log.Printf("[validate-order] Order %s is valid", data.OrderID)
	return map[string]interface{}{"valid": true, "orderId": data.OrderID}, nil
}

// chargePayment rejects amounts over $1500 (simulates card decline).
func chargePayment(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string  `json:"orderId"`
		Amount  float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	log.Printf("[charge-payment] Charging $%.2f for %s", data.Amount, data.OrderID)
	time.Sleep(300 * time.Millisecond)

	if data.Amount > 1500 {
		return nil, flo.NewNonRetryableErrorf("payment of $%.0f declined — exceeds $1500 limit", data.Amount)
	}
	return toBytes(map[string]interface{}{
		"charged": true,
		"orderId": data.OrderID,
		"amount":  data.Amount,
	})
}

// shipOrder rejects orders with id ending in "-FAIL".
func shipOrder(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string `json:"orderId"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	log.Printf("[ship-order] Shipping order %s", data.OrderID)
	time.Sleep(200 * time.Millisecond)

	if strings.HasSuffix(data.OrderID, "-FAIL") {
		return nil, flo.NewNonRetryableErrorf("shipping failed for %s — address invalid", data.OrderID)
	}
	return toBytes(map[string]interface{}{
		"shipped":    true,
		"orderId":    data.OrderID,
		"trackingId": "TRK-42",
	})
}

// validateExpense validates an expense report.
func validateExpense(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		Expense string  `json:"expense"`
		Amount  float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	log.Printf("[validate-expense] Validating expense: %s ($%.0f)", data.Expense, data.Amount)
	time.Sleep(200 * time.Millisecond)
	return toBytes(map[string]interface{}{"valid": true, "expense": data.Expense})
}

// processExpense processes an approved expense.
func processExpense(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		Expense string  `json:"expense"`
		Amount  float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	log.Printf("[process-expense] Processing expense: %s", data.Expense)
	time.Sleep(200 * time.Millisecond)
	return toBytes(map[string]interface{}{"processed": true, "expense": data.Expense})
}

// =============================================================================
// Outcome-Based Action Handlers (used by workflow e2e outcome test)
// =============================================================================

// reviewOrder returns a named outcome based on amount thresholds.
//
//	amount < 100  → "approved"
//	amount >= 500 → "rejected"
//	otherwise     → "needs_review"
func reviewOrder(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string  `json:"orderId"`
		Amount  float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}
	log.Printf("[review-order] Reviewing order %s ($%.0f)", data.OrderID, data.Amount)
	time.Sleep(200 * time.Millisecond)

	if data.Amount < 100 {
		return actx.Result("approved", map[string]interface{}{
			"orderId": data.OrderID, "decision": "auto-approved",
		})
	} else if data.Amount >= 500 {
		return actx.Result("rejected", map[string]interface{}{
			"orderId": data.OrderID, "reason": "amount too high",
		})
	}
	return actx.Result("needs_review", map[string]interface{}{
		"orderId": data.OrderID, "note": "manual review required",
	})
}

// fulfillOrder — plain object return.
func fulfillOrder(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string `json:"orderId"`
	}
	_ = actx.Into(&data)
	log.Printf("[fulfill-order] Fulfilling order %s", data.OrderID)
	time.Sleep(200 * time.Millisecond)
	return map[string]interface{}{"fulfilled": true, "orderId": data.OrderID}, nil
}

// notifyRejection — plain object return.
func notifyRejection(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string `json:"orderId"`
		Reason  string `json:"reason"`
	}
	_ = actx.Into(&data)
	log.Printf("[notify-rejection] Notifying rejection for %s: %s", data.OrderID, data.Reason)
	time.Sleep(200 * time.Millisecond)
	return map[string]interface{}{"notified": true, "orderId": data.OrderID}, nil
}

// manualReview — plain object return.
func manualReview(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID string `json:"orderId"`
	}
	_ = actx.Into(&data)
	log.Printf("[manual-review] Queuing order %s for manual review", data.OrderID)
	time.Sleep(200 * time.Millisecond)
	return map[string]interface{}{"queued": true, "orderId": data.OrderID}, nil
}

// =============================================================================
// Order-Processing Workflow Actions (used by order-workflow.yaml)
// =============================================================================

// validatePayment validates payment info before charging.
func validatePayment(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID    string  `json:"orderId"`
		CustomerID string  `json:"customerId"`
		Amount     float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, flo.NewNonRetryableErrorf("invalid input: %v", err)
	}
	log.Printf("[validate-payment] Validating payment for order %s, customer %s ($%.2f)",
		data.OrderID, data.CustomerID, data.Amount)
	time.Sleep(200 * time.Millisecond)

	if data.Amount <= 0 {
		return nil, flo.NewNonRetryableErrorf("invalid amount: $%.2f", data.Amount)
	}
	return map[string]interface{}{
		"valid":      true,
		"orderId":    data.OrderID,
		"customerId": data.CustomerID,
		"amount":     data.Amount,
	}, nil
}

// processPayment charges the customer's payment method.
func processPayment(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID    string  `json:"orderId"`
		CustomerID string  `json:"customerId"`
		Amount     float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, flo.NewNonRetryableErrorf("invalid input: %v", err)
	}
	log.Printf("[process-payment] Charging $%.2f for order %s", data.Amount, data.OrderID)
	time.Sleep(300 * time.Millisecond)

	if data.Amount > 5000 {
		return nil, flo.NewNonRetryableErrorf("payment of $%.0f declined — exceeds limit", data.Amount)
	}
	// Simulate primary processor failure for orders starting with "FB-"
	// This triggers the plan to fall back to process-payment-fallback.
	if strings.HasPrefix(data.OrderID, "FB-") {
		return nil, flo.NewNonRetryableErrorf("primary processor unavailable for order %s", data.OrderID)
	}
	return map[string]interface{}{
		"charged":       true,
		"orderId":       data.OrderID,
		"transactionId": fmt.Sprintf("TXN-%d", time.Now().UnixMilli()),
		"amount":        data.Amount,
	}, nil
}

// processPaymentFallback is a fallback payment processor (used by @plan/payment).
func processPaymentFallback(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID    string  `json:"orderId"`
		CustomerID string  `json:"customerId"`
		Amount     float64 `json:"amount"`
	}
	if err := actx.Into(&data); err != nil {
		return nil, flo.NewNonRetryableErrorf("invalid input: %v", err)
	}
	log.Printf("[process-payment-fallback] Charging $%.2f for order %s via fallback", data.Amount, data.OrderID)
	time.Sleep(500 * time.Millisecond)

	if data.Amount > 5000 {
		return nil, flo.NewNonRetryableErrorf("fallback payment of $%.0f declined — exceeds limit", data.Amount)
	}
	return map[string]interface{}{
		"charged":       true,
		"orderId":       data.OrderID,
		"transactionId": fmt.Sprintf("FB-TXN-%d", time.Now().UnixMilli()),
		"amount":        data.Amount,
		"provider":      "fallback",
	}, nil
}

// sendConfirmation sends an order confirmation notification.
func sendConfirmation(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID    string `json:"orderId"`
		CustomerID string `json:"customerId"`
	}
	_ = actx.Into(&data)
	log.Printf("[send-confirmation] Sending confirmation for order %s to customer %s",
		data.OrderID, data.CustomerID)
	time.Sleep(200 * time.Millisecond)
	return map[string]interface{}{
		"sent":    true,
		"orderId": data.OrderID,
		"channel": "email",
	}, nil
}

// sendRejection sends an order rejection notification.
func sendRejection(actx *flo.ActionContext) (interface{}, error) {
	var data struct {
		OrderID    string `json:"orderId"`
		CustomerID string `json:"customerId"`
	}
	_ = actx.Into(&data)
	log.Printf("[send-rejection] Sending rejection for order %s to customer %s",
		data.OrderID, data.CustomerID)
	time.Sleep(200 * time.Millisecond)
	return map[string]interface{}{
		"sent":    true,
		"orderId": data.OrderID,
		"reason":  "payment_declined",
	}, nil
}

// =============================================================================
// Scheduled Workflow Actions
// =============================================================================

// reconcileSpend simulates a periodic spend reconciliation check.
func reconcileSpend(actx *flo.ActionContext) (interface{}, error) {
	log.Printf("[reconcile-spend] Running spend reconciliation (attempt %d)", actx.Attempt())
	time.Sleep(500 * time.Millisecond)

	// Simulate checking budgets and correcting discrepancies
	checked := 12 + time.Now().UnixMilli()%8
	corrected := time.Now().UnixMilli() % 3

	log.Printf("[reconcile-spend] Checked %d budgets, corrected %d discrepancies", checked, corrected)
	return map[string]interface{}{
		"budgets_checked":         checked,
		"discrepancies_corrected": corrected,
		"reconciled_at":           fmt.Sprintf("%d", time.Now().UnixMilli()),
		"status":                  "clean",
	}, nil
}

// =============================================================================
// Helpers
// =============================================================================

// toBytes marshals v to JSON bytes (returning []byte so the worker sends raw).
func toBytes(v interface{}) (interface{}, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// =============================================================================
// Main
// =============================================================================

func main() {
	client := flo.NewClient(getEnv("FLO_ENDPOINT", "localhost:4453"),
		flo.WithNamespace(getEnv("FLO_NAMESPACE", "example")),
	)
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	w, err := client.NewActionWorker(flo.ActionWorkerOptions{
		Concurrency: 5,
	})
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	defer w.Close()

	// General actions
	w.MustRegisterAction("process-order", processOrder)
	w.MustRegisterAction("send-notification", sendNotification)
	w.MustRegisterAction("generate-report", generateReport)
	w.MustRegisterAction("health-check", healthCheck)

	// Workflow actions (used by workflow e2e)
	w.MustRegisterAction("validate-order", validateOrder)
	w.MustRegisterAction("charge-payment", chargePayment)
	w.MustRegisterAction("ship-order", shipOrder)
	w.MustRegisterAction("validate-expense", validateExpense)
	w.MustRegisterAction("process-expense", processExpense)

	// Outcome-based actions (used by workflow e2e outcome test)
	w.MustRegisterAction("review-order", reviewOrder)
	w.MustRegisterAction("fulfill-order", fulfillOrder)
	w.MustRegisterAction("notify-rejection", notifyRejection)
	w.MustRegisterAction("manual-review", manualReview)

	// Order-processing workflow actions (used by order-workflow.yaml)
	w.MustRegisterAction("validate-payment", validatePayment)
	w.MustRegisterAction("process-payment", processPayment)
	w.MustRegisterAction("process-payment-fallback", processPaymentFallback)
	w.MustRegisterAction("send-confirmation", sendConfirmation)
	w.MustRegisterAction("send-rejection", sendRejection)

	// Scheduled workflow actions
	w.MustRegisterAction("reconcile-spend", reconcileSpend)

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

	log.Println("Worker starting...")
	if err := w.Start(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Worker error: %v", err)
	}
}
