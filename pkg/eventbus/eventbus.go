package eventbus

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	eh "github.com/looplab/eventhorizon"
	"google.golang.org/api/option"
	"log"
	"strings"
	"sync"
	"time"
)

type Config struct {
	AppID              string
	ProjectID          string
	TopicName          string
	Region             string
	ClientOpts         []option.ClientOption
	CreateSubscription bool
}

func (c *Config) provideDefaults() {
	if c.ProjectID == "" {
		c.ProjectID = "eventhorizonEvents"
	}

	if c.Region == "" {
		c.Region = "us-central1"
	}
}

type EventBus struct {
	client       *pubsub.Client
	topic        *pubsub.Topic
	config       *Config
	registered   map[eh.EventHandlerType]struct{}
	registeredMu sync.RWMutex
	errCh        chan eh.EventBusError
	wg           sync.WaitGroup
	encoder      Encoder
}

func NewEventBus(
	config *Config) (*EventBus, error) {
	config.provideDefaults()

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, config.ProjectID,
		config.ClientOpts...)
	if err != nil {
		return nil, err
	}

	return NewEventBusWithClient(ctx, config, client)
}

func NewEventBusWithClient(ctx context.Context, config *Config,
	client *pubsub.Client) (*EventBus, error) {
	if client == nil {
		return nil, nil
	}

	b := &EventBus{
		client:     client,
		config:     config,
		registered: map[eh.EventHandlerType]struct{}{},
		errCh:      make(chan eh.EventBusError, 100),
		encoder:    &jsonEncoder{},
	}

	// Get or create the topic.
	var name string
	if b.config.TopicName != "" && b.config.AppID == "" {
		name = b.config.TopicName
	} else {
		name = b.config.AppID + "_events"
	}

	b.topic = client.Topic(name)
	if ok, err := b.topic.Exists(ctx); err != nil {
		return nil, err
	} else if !ok {
		if b.topic, err = b.client.CreateTopic(ctx, name); err != nil {
			return nil, err
		}
	}
	b.topic.EnableMessageOrdering = true

	return b, nil
}

// HandlerType implements the HandlerType method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandlerType() eh.EventHandlerType {
	return "eventbus"
}

const (
	aggregateTypeAttribute = "aggregateType"
	eventTypeAttribute     = "eventType"
)

// HandleEvent implements the HandleEvent method of the eventhorizon.EventHandler interface.
func (b *EventBus) HandleEvent(ctx context.Context, event eh.Event) error {
	e := evt{
		AggregateID:   event.AggregateID().String(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Metadata:      event.Metadata(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		if e.RawData, err = b.encoder.Marshal(event.Data()); err != nil {
			return errors.New("could not marshal event data: " + err.Error())
		}
	}

	// Marshal the event (using JSON for now).
	data, err := json.Marshal(e)
	if err != nil {
		return errors.New("could not marshal event: " + err.Error())
	}

	res := b.topic.Publish(ctx, &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			aggregateTypeAttribute: event.AggregateType().String(),
			//event.EventType().String(): "", // The event type as a key to save space when filtering.
			eventTypeAttribute: event.EventType().String(), // The event type as a key to save space when filtering.
		},
		OrderingKey: event.AggregateID().String(),
	})

	if _, err := res.Get(ctx); err != nil {
		return errors.New("could not publish event: " + err.Error())
	}

	return nil
}

// AddHandler implements the AddHandler method of the eventhorizon.EventBus interface.
func (b *EventBus) AddHandler(ctx context.Context, m eh.EventMatcher, h eh.EventHandler) error {
	if m == nil {
		return eh.ErrMissingMatcher
	}
	if h == nil {
		return eh.ErrMissingHandler
	}

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()
	if _, ok := b.registered[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	if b.config.CreateSubscription {
		// Build the subscription filter.
		filter := createFilter(m)
		if len(filter) >= 256 {
			return fmt.Errorf("match filter is longer than 256 chars: %d", len(filter))
		}

		// Get or create the subscription.
		subscriptionID := b.config.AppID + "_" + h.HandlerType().String()
		sub := b.client.Subscription(subscriptionID)
		if ok, err := sub.Exists(ctx); err != nil {
			return fmt.Errorf("could not check existing subscription: %w", err)
		} else if !ok {
			if sub, err = b.client.CreateSubscription(ctx, subscriptionID,
				pubsub.SubscriptionConfig{
					Topic:                 b.topic,
					AckDeadline:           60 * time.Second,
					Filter:                filter,
					EnableMessageOrdering: true,
					RetryPolicy: &pubsub.RetryPolicy{
						MinimumBackoff: 3 * time.Second,
					},
				},
			); err != nil {
				return fmt.Errorf("could not create subscription: %w", err)
			}
		} else if ok {
			cfg, err := sub.Config(ctx)
			if err != nil {
				return fmt.Errorf("could not get subscription config: %w", err)
			}
			if cfg.Filter != filter {
				return fmt.Errorf("the existing filter for '%s' differs, please remove to recreate", h.HandlerType())
			}
			if !cfg.EnableMessageOrdering {
				return fmt.Errorf("message ordering not enabled for subscription '%s', please remove to recreate", h.HandlerType())
			}
		}

		// Register handler.
		b.registered[h.HandlerType()] = struct{}{}

		// Handle until context is cancelled.
		b.wg.Add(1)
		go b.handle(ctx, m, h, sub)

		return nil
	}

	// Register handler.
	b.registered[h.HandlerType()] = struct{}{}

	b.wg.Add(1)
	go b.handle(ctx, m, h, nil)

	return nil
}

// Errors implements the Errors method of the eventhorizon.EventBus interface.
func (b *EventBus) Errors() <-chan eh.EventBusError {
	return b.errCh
}

// Wait for all channels to close in the event bus group
func (b *EventBus) Wait() {
	b.wg.Wait()
}

// Handles all events coming in on the channel.
func (b *EventBus) handle(ctx context.Context, m eh.EventMatcher, h eh.EventHandler, sub *pubsub.Subscription) {

	if b.config.CreateSubscription && sub != nil {
		defer b.wg.Done()
		for {
			if err := sub.Receive(ctx, b.handler(m, h)); err != nil {
				err = fmt.Errorf("could not receive: %w", err)
				select {
				case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
				default:
					log.Printf("eventhorizon: missed error in GCP event bus: %s", err)
				}
				// Retry the receive loop if there was an error.
				time.Sleep(time.Second)
				continue
			}
			return
		}
	}

	b.handler(m, h)
}

func (b *EventBus) handler(m eh.EventMatcher, h eh.EventHandler) func(ctx context.Context, msg *pubsub.Message) {
	return func(ctx context.Context, msg *pubsub.Message) {

		var e evt
		var err error

		eventType, _ := msg.Attributes[eventTypeAttribute]
		err = json.Unmarshal(msg.Data, &e)
		e.data, err = b.encoder.Unmarshal(eh.EventType(eventType), e.RawData)

		event := eh.NewEvent(
			e.EventType,
			e.data,
			e.Timestamp,
			eh.ForAggregate(
				e.AggregateType,
				uuid.MustParse(e.AggregateID),
				e.Version,
			),
			eh.WithMetadata(e.Metadata),
		)

		if err != nil {
			err = fmt.Errorf("could not unmarshal event: %w", err)
			select {
			case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx}:
			default:
				log.Printf("eventhorizon: missed error in GCP event bus: %s", err)
			}
			msg.Nack()
			return
		}

		// Ignore non-matching events.
		if !m.Match(event) {
			msg.Ack()
			return
		}

		// Handle the event if it did match.
		if err := h.HandleEvent(ctx, event); err != nil {
			err = fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err)
			select {
			case b.errCh <- eh.EventBusError{Err: err, Ctx: ctx, Event: event}:
			default:
				log.Printf("eventhorizon: missed error in GCP event bus: %s", err)
			}
			msg.Nack()
			return
		}

		msg.Ack()
	}
}

// Creates a filter in the GCP pub sub filter syntax:
// https://cloud.google.com/pubsub/docs/filtering
func createFilter(m eh.EventMatcher) string {
	switch m := m.(type) {
	case eh.MatchEvents:
		s := make([]string, len(m))
		for i, et := range m {
			s[i] = fmt.Sprintf(`attributes:"%s"`, et) // Filter event types by key to save space.
		}
		return strings.Join(s, " OR ")
	case eh.MatchAggregates:
		s := make([]string, len(m))
		for i, at := range m {
			s[i] = fmt.Sprintf(`attributes.%s="%s"`, aggregateTypeAttribute, at)
		}
		return strings.Join(s, " OR ")
	case eh.MatchAny:
		s := make([]string, len(m))
		for i, sm := range m {
			s[i] = fmt.Sprintf("(%s)", createFilter(sm))
		}
		return strings.Join(s, " OR ")
	case eh.MatchAll:
		s := make([]string, len(m))
		for i, sm := range m {
			s[i] = fmt.Sprintf("(%s)", createFilter(sm))
		}
		return strings.Join(s, " AND ")
	default:
		return ""
	}
}

// evt is the internal event used on the wire only.
type evt struct {
	EventType     eh.EventType           `json:"eventType"`
	RawData       json.RawMessage        `json:"data,omitempty"`
	data          eh.EventData           `json:"-"`
	Timestamp     time.Time              `json:"timestamp"`
	AggregateType eh.AggregateType       `json:"aggregateType"`
	AggregateID   string                 `json:"aggregateID"`
	Version       int                    `json:"version"`
	Metadata      map[string]interface{} `json:"metadata"`
	Context       map[string]interface{} `json:"context"`
}
