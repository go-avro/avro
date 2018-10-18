package avro

import (
	"bytes"
	"encoding/json"
	"strconv"
)

/*
 * this file contains a bunch of helper methods for writing bespoke JSON marshal functions.
 */

func writeFieldName(buf *bytes.Buffer, fieldname string, precedingComma bool) {
	if precedingComma {
		buf.WriteRune(',')
	}
	buf.WriteRune('"')
	buf.WriteString(fieldname)
	buf.WriteRune('"')
	buf.WriteRune(':')
}

func writeInt(buf *bytes.Buffer, fieldname string, value int, precedingComma bool) {
	writeFieldName(buf, fieldname, precedingComma)
	buf.WriteString(strconv.FormatInt(int64(value), 10))
}

func writeString(buf *bytes.Buffer, fieldname, value string, precedingComma bool) {
	writeFieldName(buf, fieldname, precedingComma)
	formatted, _ := json.Marshal(value)
	buf.Write(formatted)
}
