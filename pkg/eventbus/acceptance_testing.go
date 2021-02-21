package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kr/pretty"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/middleware/eventhandler/observer"
	"github.com/looplab/eventhorizon/mocks"
)

// AcceptanceTest is the acceptance test that all implementations of EventBus
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestEventBus(t *testing.T) {
//       bus1 := NewEventBus()
//       bus2 := NewEventBus()
//       eventbus.AcceptanceTest(t, bus1, bus2)
//   }
//
func AcceptanceTest(t *testing.T, bus1, bus2 eh.EventBus, timeout time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())

	// Error on nil matcher.
	if err := bus1.AddHandler(ctx, nil, mocks.NewEventHandler("no-matcher")); err != eh.ErrMissingMatcher {
		t.Error("the error should be correct:", err)
	}

	// Error on nil handler.
	if err := bus1.AddHandler(ctx, eh.MatchAll{}, nil); err != eh.ErrMissingHandler {
		t.Error("the error should be correct:", err)
	}

	// Error on multiple registrations.
	if err := bus1.AddHandler(ctx, eh.MatchAll{}, mocks.NewEventHandler("multi")); err != nil {
		t.Error("there should be no errer:", err)
	}
	if err := bus1.AddHandler(ctx, eh.MatchAll{}, mocks.NewEventHandler("multi")); err != eh.ErrHandlerAlreadyAdded {
		t.Error("the error should be correct:", err)
	}

	ctx = mocks.WithContextOne(ctx, "testval")

	// Without handler.
	id := uuid.New()
	timestamp := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	event1 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event1"}, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 1),
		eh.WithMetadata(map[string]interface{}{"meta": "data", "num": float64(42)}),
	)
	if err := bus1.HandleEvent(ctx, event1); err != nil {
		t.Error("there should be no error:", err)
	}

	// Event without data (tested in its own handler).
	otherHandler := mocks.NewEventHandler("other-handler")
	_ = bus1.AddHandler(ctx, eh.MatchEvents{mocks.EventOtherType}, otherHandler)

	time.Sleep(timeout) // Need to wait here for handlers to be added.

	eventWithoutData := eh.NewEvent(mocks.EventOtherType, nil, timestamp,
		eh.ForAggregate(mocks.AggregateType, uuid.New(), 1))
	if err := bus1.HandleEvent(ctx, eventWithoutData); err != nil {
		t.Error("there should be no error:", err)
	}
	expectedEvents := []eh.Event{eventWithoutData}
	if !otherHandler.Wait(timeout) {
		t.Error("did not receive event in time")
	}
	if !mocks.EqualEvents(otherHandler.Events, expectedEvents) {
		t.Error("the events were incorrect:")
		t.Log(otherHandler.Events)
		if len(otherHandler.Events) == 1 {
			t.Log(pretty.Sprint(otherHandler.Events[0]))
		}
	}

	const (
		handlerName  = "handler"
		observerName = "observer"
	)

	// Add handlers and observers.
	handlerBus1 := mocks.NewEventHandler(handlerName)
	handlerBus2 := mocks.NewEventHandler(handlerName)
	anotherHandlerBus2 := mocks.NewEventHandler("another_handler")
	observerBus1 := mocks.NewEventHandler(observerName)
	observerBus2 := mocks.NewEventHandler(observerName)
	_ = bus1.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handlerBus1)
	_ = bus2.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, handlerBus2)
	_ = bus2.AddHandler(ctx, eh.MatchEvents{mocks.EventType}, anotherHandlerBus2)
	// Add observers using the observer middleware.
	_ = bus1.AddHandler(ctx, eh.MatchAll{}, eh.UseEventHandlerMiddleware(observerBus1, observer.Middleware))
	_ = bus2.AddHandler(ctx, eh.MatchAll{}, eh.UseEventHandlerMiddleware(observerBus2, observer.Middleware))

	time.Sleep(timeout) // Need to wait here for handlers to be added.

	// Event with data.
	event2 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event2"}, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 2),
		eh.WithMetadata(map[string]interface{}{"meta": "data", "num": float64(42)}),
	)
	if err := bus1.HandleEvent(ctx, event2); err != nil {
		t.Error("there should be no error:", err)
	}

	// Check for correct event in handler 1 or 2.
	expectedEvents = []eh.Event{event2}
	if !(handlerBus1.Wait(timeout) || handlerBus2.Wait(timeout)) {
		t.Error("did not receive event in time")
	}
	if !(mocks.EqualEvents(handlerBus1.Events, expectedEvents) ||
		mocks.EqualEvents(handlerBus2.Events, expectedEvents)) {
		t.Error("the events were incorrect:")
		t.Log(handlerBus1.Events)
		t.Log(handlerBus2.Events)
		if len(handlerBus1.Events) == 1 {
			t.Log(pretty.Sprint(handlerBus1.Events[0]))
		}
		if len(handlerBus2.Events) == 1 {
			t.Log(pretty.Sprint(handlerBus2.Events[0]))
		}
	}
	if mocks.EqualEvents(handlerBus1.Events, handlerBus2.Events) {
		t.Error("only one handler should receive the events")
	}
	correctCtx1 := false
	if val, ok := mocks.ContextOne(handlerBus1.Context); ok && val == "testval" {
		correctCtx1 = true
	}
	correctCtx2 := false
	if val, ok := mocks.ContextOne(handlerBus2.Context); ok && val == "testval" {
		correctCtx2 = true
	}
	if !correctCtx1 && !correctCtx2 {
		t.Error("the context should be correct")
	}

	// Check the other handler.
	if !anotherHandlerBus2.Wait(timeout) {
		t.Error("did not receive event in time")
	}
	if !mocks.EqualEvents(anotherHandlerBus2.Events, expectedEvents) {
		t.Error("the events were incorrect:")
		t.Log(anotherHandlerBus2.Events)
	}
	if val, ok := mocks.ContextOne(anotherHandlerBus2.Context); !ok || val != "testval" {
		t.Error("the context should be correct:", anotherHandlerBus2.Context)
	}

	// Check observer 1.
	if !observerBus1.Wait(timeout) {
		t.Error("did not receive event in time")
	}
	for i, event := range observerBus1.Events {
		if err := mocks.CompareEvents(event, expectedEvents[i]); err != nil {
			t.Error("the event was incorrect:", err)
		}
	}
	if val, ok := mocks.ContextOne(observerBus1.Context); !ok || val != "testval" {
		t.Error("the context should be correct:", observerBus1.Context)
	}

	// Check observer 2.
	if !observerBus2.Wait(timeout) {
		t.Error("did not receive event in time")
	}
	for i, event := range observerBus2.Events {
		if err := mocks.CompareEvents(event, expectedEvents[i]); err != nil {
			t.Error("the event was incorrect:", err)
		}
	}
	if val, ok := mocks.ContextOne(observerBus2.Context); !ok || val != "testval" {
		t.Error("the context should be correct:", observerBus2.Context)
	}

	// Test async errors from handlers.
	errorHandler := mocks.NewEventHandler("error_handler")
	errorHandler.Err = errors.New("handler error")
	_ = bus1.AddHandler(ctx, eh.MatchAll{}, errorHandler)

	time.Sleep(timeout) // Need to wait here for handlers to be added.

	event3 := eh.NewEvent(mocks.EventType, &mocks.EventData{Content: "event3"}, timestamp,
		eh.ForAggregate(mocks.AggregateType, id, 3),
		eh.WithMetadata(map[string]interface{}{"meta": "data", "num": float64(42)}),
	)
	if err := bus1.HandleEvent(ctx, event3); err != nil {
		t.Error("there should be no error:", err)
	}
	select {
	case <-time.After(timeout):
		t.Error("there should be an async error")
	case err := <-bus1.Errors():
		// Good case.
		if err.Error() != "could not handle event (error_handler): handler error: (Event@3)" {
			t.Error("incorrect error sent on event bus:", err)
		}
	}

	// Cancel all handlers and wait.
	cancel()
	bus1.Wait()
	bus2.Wait()
}