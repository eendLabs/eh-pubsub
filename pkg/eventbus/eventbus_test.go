package eventbus

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	eheventbus "github.com/looplab/eventhorizon/eventbus"
)

func TestEventBusIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Connect to localhost if not running inside docker
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		if err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8793");
		err != nil {
			t.Fatal("could not set PUBSUB_EMULATOR_HOST", err)
		}
	}

	// Get a random app ID.
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	appID := "app-" + hex.EncodeToString(b)

	busConfig1 := &Config{
		ProjectID: "project_id",
		AppID: appID,
		CreateSubscription: true,
	}
	//bus1, err := NewEventBus("project_id", appID)
	bus1, err := NewEventBus(busConfig1)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	busConfig2 := &Config{
		ProjectID: "project_id",
		AppID: appID,
		CreateSubscription: true,
	}
	bus2, err := NewEventBus(busConfig2)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	AcceptanceTest(t, bus1, bus2, time.Second)
}

func TestEventBusLoadTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Connect to localhost if not running inside docker
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		if err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8793");
			err != nil {
			t.Fatal("could not set PUBSUB_EMULATOR_HOST", err)
		}
	}

	// Get a random app ID.
	bts := make([]byte, 8)
	if _, err := rand.Read(bts); err != nil {
		t.Fatal(err)
	}
	appID := "app-" + hex.EncodeToString(bts)

	busConfig1 := &Config{
		ProjectID: "project_id",
		AppID: appID,
		CreateSubscription: true,
	}
	bus1, err := NewEventBus(busConfig1)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	busConfig2 := &Config{
		ProjectID: "project_id",
		TopicName: appID,
	}
	bus2, err := NewEventBus(busConfig2)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	eheventbus.LoadTest(t, bus1)
	eheventbus.LoadTest(t, bus2)
}

func BenchmarkEventBus(b *testing.B) {
	// Connect to localhost if not running inside docker
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		if err := os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8793");
			err != nil {
			b.Fatal("could not set PUBSUB_EMULATOR_HOST", err)
		}
	}

	// Get a random app ID.
	bts := make([]byte, 8)
	if _, err := rand.Read(bts); err != nil {
		b.Fatal(err)
	}
	appID := "app-" + hex.EncodeToString(bts)

	busConfig1 := &Config{
		ProjectID: "project_id",
		AppID: appID,
		CreateSubscription: true,
	}
	bus1, err := NewEventBus(busConfig1)
	if err != nil {
		b.Fatal("there should be no error:", err)
	}

	busConfig2 := &Config{
		ProjectID: "project_id",
		TopicName: appID,
	}
	bus2, err := NewEventBus(busConfig2)
	if err != nil {
		b.Fatal("there should be no error:", err)
	}

	eheventbus.Benchmark(b, bus1)
	eheventbus.Benchmark(b, bus2)
}
