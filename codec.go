package obox

import (
  "io"
  "github.com/reusee/codec-go"
)

var CODEC_HANDLE = &codec.BincHandle{}

func encode(buf io.Writer, e interface{}) (error) {
  return codec.NewEncoder(buf, CODEC_HANDLE).Encode(e)
}

func decode(buf io.Reader, e interface{}) (error) {
  return codec.NewDecoder(buf, CODEC_HANDLE).Decode(e)
}
