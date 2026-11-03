package jobs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// StatusChannel is the PostgreSQL channel that the jobs table trigger
// (migration 013) notifies with the new status whenever it changes.
const StatusChannel = "job_status_changed"

// Notifier listens for job status changes with PostgreSQL LISTEN and
// wakes the workers waiting for that status. It replaces tight polling:
// a worker sleeps until a job reaches its input status, with a slow
// fallback poll in case a notification is missed.
type Notifier struct {
	DSN string

	mu          sync.Mutex
	subscribers map[string][]chan struct{}
}

// NewNotifier creates a notifier that connects with dsn.
func NewNotifier(dsn string) *Notifier {
	return &Notifier{
		DSN:         dsn,
		subscribers: make(map[string][]chan struct{}),
	}
}

// Subscribe returns a channel signalled when a job enters status.
// Signals are coalesced: one pending signal means "check for work".
func (n *Notifier) Subscribe(status string) <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan struct{}, 1)
	n.subscribers[status] = append(n.subscribers[status], ch)
	return ch
}

// Run keeps a dedicated LISTEN connection open until ctx is cancelled,
// reconnecting with backoff. After each (re)connect every subscriber is
// woken, because notifications sent while disconnected are lost.
func (n *Notifier) Run(ctx context.Context) {
	backoff := time.Second

	for ctx.Err() == nil {
		err := n.listen(ctx)
		if ctx.Err() != nil {
			return
		}

		slog.Warn("job notifier disconnected; retrying", "error", err, "retry_in", backoff.String())

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (n *Notifier) listen(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, n.DSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))

	if _, err := conn.Exec(ctx, "LISTEN "+StatusChannel); err != nil {
		return err
	}

	slog.Info("listening for job status changes", "channel", StatusChannel)
	n.wakeAll()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		n.wake(notification.Payload)
	}
}

func (n *Notifier) wake(status string) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, ch := range n.subscribers[status] {
		signal(ch)
	}
}

func (n *Notifier) wakeAll() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, channels := range n.subscribers {
		for _, ch := range channels {
			signal(ch)
		}
	}
}

// signal sends without blocking; a pending signal is enough.
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
