package vedis

import (
	"testing"
)

func TestSetGetString(t *testing.T) {
	store, err := Open(":mem:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Exec("SET test 'Hello World'"); err != nil {
		t.Fatal(err)
	}
	result, err := store.ExecResult("GET test")
	if err != nil {
		t.Fatal(err)
	}
	if x := result.String(); x != "Hello World" {
		t.Fatalf("%s should equal Hello World", x)
	}
}

func TestSetGetInt(t *testing.T) {
	store, err := Open(":mem:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Exec("SET x 123"); err != nil {
		t.Fatal(err)
	}
	result, err := store.ExecResult("GET x")
	if err != nil {
		t.Fatal(err)
	}
	if x := result.Int(); x != 123 {
		t.Fatalf("%d should equal 123", x)
	}
}

func TestSetGetInt64(t *testing.T) {
	store, err := Open(":mem:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Exec("SET x 123"); err != nil {
		t.Fatal(err)
	}
	result, err := store.ExecResult("GET x")
	if err != nil {
		t.Fatal(err)
	}
	if x := result.Int(); x != 123 {
		t.Fatalf("%d should equal 123", x)
	}
}

func TestSetGetBool(t *testing.T) {
	store, err := Open(":mem:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Exec("SET truthy true"); err != nil {
		t.Fatal(err)
	}
	if err := store.Exec("SET falsy false"); err != nil {
		t.Fatal(err)
	}
	result, err := store.ExecResult("GET truthy")
	if err != nil {
		t.Fatal(err)
	}
	if x := result.Bool(); x != true {
		t.Fatalf("%v should equal true", x)
	}
	result, err = store.ExecResult("GET falsy")
	if err != nil {
		t.Fatal(err)
	}
	if x := result.Bool(); x != false {
		t.Fatalf("%v should equal false", x)
	}
}

func TestSetGetArray(t *testing.T) {
	store, err := Open(":mem:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, err := store.ExecResult("CMD_LIST")
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsArray() {
		t.Fatal("should be an array")
	}
	var commands []string
	arr := result.Array()
	for e := arr.Next(); e != nil; e = arr.Next() {
		commands = append(commands, e.String())
	}
}

func TestKvStoreFetch(t *testing.T) {
	store, err := Open(":mem:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.KvStore([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	var val []byte
	val, err = store.KvFetch([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "value" {
		t.Fatalf("incorrect value")
	}
}
