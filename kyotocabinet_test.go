package obox

import (
  "testing"
)

func TestKcDb(t *testing.T) {
  db, err := OpenKcDb("foo")
  if err != nil { t.Fatal("open fail", err) }
  testDb(t, db)
}

func BenchmarkKcDbIter(b *testing.B) {
  db, err := OpenKcDb("bar")
  if err != nil { b.Fatal("open fail", err) }
  benchmarkDb(b, db)
}

