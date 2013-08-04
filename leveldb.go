package obox

/*
#include <leveldb/c.h>
#include <stdlib.h>
#cgo LDFLAGS: -lleveldb
*/
import "C"
import (
  "unsafe"
  "errors"
  "fmt"
  "bytes"
  "reflect"
)

const (
  levelFalse = C.uchar(0)
)

type Leveldb struct {
  cdb *C.leveldb_t
  read_options *C.leveldb_readoptions_t
  write_options *C.leveldb_writeoptions_t
}

func OpenLeveldb(dir string) (*Leveldb, error) {
  options := C.leveldb_options_create()
  C.leveldb_options_set_create_if_missing(options, C.uchar(1))
  cDir := C.CString(dir)
  defer C.free(unsafe.Pointer(cDir))
  var err *C.char
  db := C.leveldb_open(options, cDir, &err)
  if err != nil {
    return nil, errors.New(fmt.Sprintf("%s: %s", err, dir))
  }
  C.leveldb_free(unsafe.Pointer(err)); err = nil
  leveldb := &Leveldb{
    cdb: db,
    read_options: C.leveldb_readoptions_create(),
    write_options: C.leveldb_writeoptions_create(),
  }
  return leveldb, nil
}

func (self *Leveldb) Close() error {
  C.leveldb_close(self.cdb)
  return nil
}

func (self *Leveldb) Count() (n int64) {
  self.Iter(func(string, Getter) bool {
    n++
    return true
  })
  return
}

func (self *Leveldb) Iter(fun func(string, Getter) bool) {
  iterator := C.leveldb_create_iterator(self.cdb, self.read_options)
  defer C.leveldb_iter_destroy(iterator)
  var keyLen C.size_t
  var keyValue *C.char
  var valueLen C.size_t
  var valueValue *C.char
  var ret bool
  for C.leveldb_iter_seek_to_first(iterator); C.leveldb_iter_valid(iterator) != levelFalse; C.leveldb_iter_next(iterator) {
    keyValue = C.leveldb_iter_key(iterator, &keyLen)
    key := string(C.GoBytes(unsafe.Pointer(keyValue), C.int(keyLen)))
    valueValue = C.leveldb_iter_value(iterator, &valueLen)
    value := C.GoBytes(unsafe.Pointer(valueValue), C.int(valueLen))
    r := bytes.NewReader(value)
    ret = fun(key, func(e interface{}) error {
      return decode(r, e)
    })
    if ret == false {
      break
    }
  }
}

func (self *Leveldb) Get(key string, obj interface{}) error {
  cKey := C.CString(key)
  defer C.free(unsafe.Pointer(cKey))
  var valueLen C.size_t
  var cerr *C.char
  valueValue := C.leveldb_get(self.cdb, self.read_options, cKey, C.size_t(len(key)), &valueLen, &cerr)
  if cerr != nil {
    return errors.New(fmt.Sprintf("%s", cerr))
  }
  bs := C.GoBytes(unsafe.Pointer(valueValue), C.int(valueLen))
  r := bytes.NewReader(bs)
  err := decode(r, obj)
  if err != nil { return err }
  return nil
}

func (self *Leveldb) Set(key string, obj interface{}) error {
  buf := new(bytes.Buffer)
  err := encode(buf, obj)
  if err != nil { return err }
  value := buf.Bytes()
  cKey := C.CString(key)
  defer C.free(unsafe.Pointer(cKey))
  header := (*reflect.SliceHeader)(unsafe.Pointer(&value))
  var cerr *C.char
  C.leveldb_put(self.cdb, self.write_options, cKey, C.size_t(len(key)), (*C.char)(unsafe.Pointer(header.Data)), C.size_t(header.Len), &cerr)
  if cerr != nil {
    return errors.New(fmt.Sprintf("%s", cerr))
  }
  return nil
}

func (self *Leveldb) SetDefault(key string, obj interface{}) (bool, error) {
  return false, nil //TODO
}
