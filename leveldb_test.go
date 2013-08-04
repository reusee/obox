package obox

import (
  "testing"
)

func TestLeveldb(t *testing.T) {
  db, err := OpenLeveldb("levelfoo")
  if err != nil { t.Fatal("open fail", err) }
  testDb(t, db)
}

func BenchmarkLeveldbIter(b *testing.B) {
  db, err := OpenLeveldb("levelfoo")
  if err != nil { b.Fatal("open fail", err) }
  benchmarkDb(b, db)
}

