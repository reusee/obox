package obox

//#include <stdlib.h>
//#include <kclangc.h>
//#cgo LDFLAGS: -lkyotocabinet
import "C"
import (
  "errors"
  "unsafe"
  "fmt"
  "bytes"
  "reflect"
)

const kcFalse = C.int32_t(0)
const kcTrue = C.int32_t(1)

type KcDb struct {
  cdb *C.KCDB
}

func OpenKcDb(dbFile string) (*KcDb, error) {
  db := new(KcDb)
  cdb := C.kcdbnew()
  cDbFile := C.CString(dbFile + "#type=kch#opts=c#zcomp=lzma#msiz=536870912")
  defer C.free(unsafe.Pointer(cDbFile))
  if C.kcdbopen(cdb, cDbFile, C.KCOWRITER | C.KCOCREATE | C.KCOTRYLOCK) == kcFalse {
    errCode := C.kcdbecode(cdb)
    return nil, errors.New(fmt.Sprintf("open: %s", C.GoString(C.kcecodename(errCode))))
  }
  db.cdb = cdb
  return db, nil
}

func (self *KcDb) Set(key string, obj interface{}) (error) {
  var err error
  buf := new(bytes.Buffer)
  err = encode(buf, obj)
  if err != nil { return err }
  bytes := buf.Bytes()
  cKey := C.CString(key)
  defer C.free(unsafe.Pointer(cKey))
  header := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
  if C.kcdbset(self.cdb, cKey, C.size_t(len(key)), (*C.char)(unsafe.Pointer(header.Data)), C.size_t(header.Len)) == kcFalse {
    return errors.New(fmt.Sprintf("set: %s", key))
  }
  return nil
}

func (self *KcDb) SetDefault(key string, obj interface{}) (bool, error) {
  cKey := C.CString(key)
  defer C.free(unsafe.Pointer(cKey))
  if C.kcdbcheck(self.cdb, cKey, C.size_t(len(key))) != C.int32_t(-1) {
    return false, nil
  }
  buf := new(bytes.Buffer)
  err := encode(buf, obj)
  if err != nil {
    return false, err
  }
  bytes := buf.Bytes()
  header := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
  if C.kcdbset(self.cdb, cKey, C.size_t(len(key)), (*C.char)(unsafe.Pointer(header.Data)), C.size_t(header.Len)) == kcFalse {
    return false, errors.New(fmt.Sprintf("set: %s", key))
  }
  return true, nil
}

func (self *KcDb) Get(key string, obj interface{}) (error) {
  var b []byte
  var err error
  cKey := C.CString(key)
  defer C.free(unsafe.Pointer(cKey))
  var vSize C.size_t
  vBuf := C.kcdbget(self.cdb, cKey, C.size_t(len(key)), &vSize)
  if vBuf == nil { return errors.New(fmt.Sprintf("get: %s", key)) }
  defer C.kcfree(unsafe.Pointer(vBuf))
  b = C.GoBytes(unsafe.Pointer(vBuf), C.int(vSize))
  r := bytes.NewReader(b)
  err = decode(r, obj)
  if err != nil { return err }
  return nil
}

func (self *KcDb) Iter(fun func(string, Getter) bool) {
  cur := C.kcdbcursor(self.cdb)
  C.kccurjump(cur)
  var kSize, vSize C.size_t
  var vBuff, kBuff *C.char
  var ret bool
  for {
    kBuff = C.kccurget(cur, &kSize, &vBuff, &vSize, kcTrue)
    if kBuff == nil {
      C.kcfree(unsafe.Pointer(kBuff))
      break
    }
    key := string(C.GoBytes(unsafe.Pointer(kBuff), C.int(kSize)))
    value := C.GoBytes(unsafe.Pointer(vBuff), C.int(vSize))
    r := bytes.NewReader(value)
    ret = fun(key, func(e interface{}) error {
      return decode(r, e)
    })
    C.kcfree(unsafe.Pointer(kBuff))
    if ret == false {
      break
    }
  }
  C.kccurdel(cur)
}

func (self *KcDb) Count() int64 {
  return int64(C.kcdbcount(self.cdb))
}

func (self *KcDb) Close() error {
  defer C.kcdbdel(self.cdb)
  if C.kcdbclose(self.cdb) == kcFalse {
    return errors.New(fmt.Sprintf("close: %s", C.GoString(C.kcecodename(C.kcdbecode(self.cdb)))))
  }
  return nil
}
