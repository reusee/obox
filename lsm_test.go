package obox

import (
  "testing"
)

func TestLsmDb(t *testing.T) {
  db, err := OpenLsmDb("lsmfoo")
  if err != nil { t.Fatal("open file", err) }
  testDb(t, db)
}

func BenchmarkLsmDbIter(b *testing.B) {
  db, err := OpenLsmDb("lsmfoo")
  if err != nil { b.Fatal("open fail", err) }
  benchmarkDb(b, db)
}
