package dynstore

import (
	"kvstore/kvstore"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var store kvstore.Store

func TestMain(m *testing.M) {
	if os.Getenv("CI") == "" {
		// TODO: Run tests against fake dynanmo so these can be added to the CI.
		sess := session.Must(session.NewSession())
		client := dynamodb.New(sess, aws.NewConfig().WithRegion("us-west-2"))
		store = New(client, "dev-consumer-store")
		m.Run()
	}
}

func TestSetIf(t *testing.T) {
	err := store.Set("tests/setif", "initial")
	FailIfError("Failed to Set 'setif'", t, err)

	err = store.SetIf("tests/setif", "update", "anotherValue")
	if err != kvstore.ErrMismatch {
		t.Errorf("expected ErrMismatch was %+v", err)
	}

	err = store.SetIf("tests/setif", "update", "initial")
	FailIfError("Failed to Set 'setif' with new value", t, err)
}

func TestSetIfNotExists(t *testing.T) {
	// Make sure old data doesn't exist.
	_ = store.Delete("tests/setifnotexits")
	_ = store.Delete("tests/setifnotexits_other")

	expected := "initial"

	err := store.SetIfNotExists("tests/setifnotexits", expected)
	FailIfError("Failed to Set 'setifnotexits'", t, err)

	err = store.SetIfNotExists("tests/setifnotexits", "update")
	if err != kvstore.ErrMismatch {
		t.Errorf("expected ErrMismatch was %+v", err)
	}

	var actual string
	err = store.Get("tests/setifnotexits", &actual)
	FailIfError("Failed to Get 'tests/setifnotexits'", t, err)

	if actual != expected {
		t.Errorf("Expected '%s' was '%s'", expected, actual)
	}

	err = store.SetIfNotExists("tests/setifnotexits_other", "another")
	FailIfError("Failed to Set 'setifnotexits_other'", t, err)
}

func TestString(t *testing.T) {
	expected := "this is a string"
	err := store.Set("tests/a-string", expected)
	FailIfError("Failed to Set 'a-string'", t, err)

	var actual string
	err = store.Get("tests/a-string", &actual)
	FailIfError("Failed to Get 'a-string'", t, err)

	if actual != expected {
		t.Errorf("Expected '%s' was '%s'", expected, actual)
	}
}

type book struct {
	Title string
	Pages int
}

func TestStruct(t *testing.T) {
	expected := book{"Neuromancer", 271}
	err := store.Set("tests/a-struct", expected)
	FailIfError("Failed to Set 'a-struct'", t, err)

	var actual book
	err = store.Get("tests/a-struct", &actual)
	FailIfError("Failed to Get 'a-struct'", t, err)

	if actual != expected {
		t.Errorf("Expected '%v' was '%v'", expected, actual)
	}
}

func TestGetTime(t *testing.T) {
	expected := time.Now()
	err := store.Set("tests/test-time", expected)
	FailIfError("Failed to see 'test-time'", t, err)

	actual, err := store.GetTime("tests/test-time")
	FailIfError("Failed to GetTime 'test-time'", t, err)

	if !actual.Equal(expected) {
		t.Errorf("Expected '%s' was '%s'", expected, actual)
	}
}

func TestGetUnknownTime(t *testing.T) {
	actual, err := store.GetTime("tests/unknown-time")
	FailIfError("Failed to GetTime 'unknown-time'", t, err)

	if !actual.IsZero() {
		t.Errorf("Expected time to be zero was '%s'", actual)
	}
}

func TestGetInt32BadType(t *testing.T) {
	expected := "123"
	err := store.Set("tests/another-number", expected)
	FailIfError("Failed to Set 'another-number'", t, err)

	v, err := store.GetInt32("tests/another-number")
	if err == nil {
		t.Errorf("Fetching an int config as a string should fail, got '%+v' type %s",
			v, reflect.TypeOf(v))
	}
}

func TestDelete(t *testing.T) {
	expected := "this is a string"
	err := store.Set("tests/a-string", expected)
	FailIfError("Set error", t, err)

	var actual string
	err = store.Get("tests/a-string", &actual)
	FailIfError("Retrieval error", t, err)

	err = store.Delete("tests/a-string")
	if err != nil {
		t.Errorf("Failed to delete string 'a-string'")
	}

	retrieved, _ := store.GetString("tests/a-string")
	if retrieved != "" {
		t.Errorf("Retrieved deleted value %s", retrieved)
	}
}

// FailIfError will fail a test if any of the error arguments are not nil.
func FailIfError(msg string, t *testing.T, err ...error) {
	for _, e := range err {
		if e != nil {
			if t != nil {
				t.Fatalf("Unexpected error: %s: %+v", msg, e)
			} else {
				panic(e)
			}
		}
	}
}
