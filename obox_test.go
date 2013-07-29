package obox

import (
  "testing"
)

type foo struct {
  I int
  S string
}

func TestKcDb(t *testing.T) {
  db, err := OpenKcDb("foo")
  if err != nil { t.Fatal("open fail", err) }
  defer db.Close()
  err = db.Set("foo", foo{5, "foo"})
  if err != nil { t.Fatal("set fail", err) }
  var o foo
  err = db.Get("foo", &o)
  if err != nil { t.Fatal("get fail", err) }
  p("%v\n", o)
  if o.I != 5 { t.Fatal("get wrong") }
  if o.S != "foo" { t.Fatal("get wrong") }
}
