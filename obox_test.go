package obox

import (
  "testing"
  "time"
  "fmt"
)

type foo struct {
  I int
  S string
}

func testDb(t *testing.T, db Db) {
  defer db.Close()
  err := db.Set("foo", foo{5, "foo"})
  if err != nil { t.Fatal("set fail", err) }
  var o foo
  err = db.Get("foo", &o)
  if err != nil { t.Fatal("get fail", err) }
  p("%v\n", o)
  if o.I != 5 { t.Fatal("get wrong") }
  if o.S != "foo" { t.Fatal("get wrong") }
}

func TestKcDb(t *testing.T) {
  db, err := OpenKcDb("foo")
  if err != nil { t.Fatal("open fail", err) }
  testDb(t, db)
}

func TestLeveldb(t *testing.T) {
  db, err := OpenLeveldb("levelfoo")
  if err != nil { t.Fatal("open fail", err) }
  testDb(t, db)
}

type bar struct {
  I int
  S string
  M map[int]string
  U struct{I int; S string}
  A []string
}

func benchmarkDb(b *testing.B, db Db) {
  fmt.Printf("\n")
  n := 200000
  baar := &bar{
    5,
    "foobarbaz",
    map[int]string{
      1: "foo",
      2: "bar",
      3: "baz",
    },
    struct{I int; S string}{5, "foo"},
    []string{"foo", "bar", "baz", "qux", "quux"},
  }
  defer db.Close()
  t0 := time.Now()
  for i := 0; i < n; i++ {
    db.Set(fmt.Sprintf("bar-%d", i), baar)
  }
  delta := time.Now().Sub(t0)
  fmt.Printf("%v %v\n", delta, delta / time.Duration(n))

  t0 = time.Now()
  i := 0
  db.Iter(func(key string, get Getter) bool {
    var o bar
    get(&o)
    if o.S != "foobarbaz" { b.Fail() }
    i++
    if i == n { return false }
    return true
  })
  delta = time.Now().Sub(t0)
  fmt.Printf("%v %v\n", delta, delta / time.Duration(n))
}

func BenchmarkKcDbIter(b *testing.B) {
  db, err := OpenKcDb("bar")
  if err != nil { b.Fatal("open fail", err) }
  benchmarkDb(b, db)
}

func BenchmarkLeveldbIter(b *testing.B) {
  db, err := OpenLeveldb("levelfoo")
  if err != nil { b.Fatal("open fail", err) }
  benchmarkDb(b, db)
}
