package generation

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func readIDs(kind string) string { return kind + "_" + strings.Repeat("a", 32) }
func testReadRequest() Request {
	return Request{Version: 1, Query: QueryChildren, RepositoryID: readIDs("repo"), SourceID: readIDs("source"), GenerationID: readIDs("generation")}
}
func testReadResponse() Response {
	n := Decimal(9007199254740993)
	return Response{Kind: ModelRecords, Query: QueryChildren, RepositoryID: readIDs("repo"), SourceID: readIDs("source"), GenerationID: readIDs("generation"), Entries: []EntryDTO{{GenerationID: readIDs("generation"), ID: readIDs("entry"), ComponentBase64: "YQ==", Kind: "file", Outcome: "stored", LogicalFileID: &n, Metadata: MetadataDTO{MtimeNS: &n}}}}
}

func TestGenerationDTOExactRoundTrips(t *testing.T) {
	for _, raw := range [][]byte{[]byte("A"), []byte("a"), []byte(" leading "), []byte("é"), []byte("e\u0301"), []byte("\\"), []byte("\n"), []byte("�"), {0xff, 0xfe}, bytes.Repeat([]byte{0xfd}, 255)} {
		r := testReadResponse()
		r.Entries[0].ComponentBase64 = base64.StdEncoding.EncodeToString(raw)
		b, e := EncodeResponse(r)
		if e != nil {
			t.Fatal(e)
		}
		again, e := DecodeResponse(b)
		if e != nil || !reflect.DeepEqual(r, again) {
			t.Fatal("lossy response", e)
		}
	}
	for _, kind := range []string{"deepest", "longest"} {
		t.Run(kind, func(t *testing.T) {
			q := testReadRequest()
			q.Query = QuerySelector
			s := selector()
			if kind == "deepest" {
				for range Depth {
					s.Components = append(s.Components, "YQ==")
				}
			} else {
				for range 11 {
					s.Components = append(s.Components, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), 255)))
				}
				s.Components = append(s.Components, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("a"), 254)), "YQ==")
			}
			q.Selector = &s
			b, e := EncodeRequest(q)
			if e != nil {
				t.Fatal(e)
			}
			again, e := DecodeRequest(b)
			if e != nil || !reflect.DeepEqual(q, again) {
				t.Fatal("selector request", e)
			}
		})
	}
}

func TestGenerationDTORefusals(t *testing.T) {
	q := testReadRequest()
	raw, e := EncodeRequest(q)
	if e != nil {
		t.Fatal(e)
	}
	cases := map[string][]byte{
		"duplicate":               bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"unknown":                 bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"filter":"anything"`), 1),
		"case-alias":              bytes.Replace(raw, []byte(`"version":1`), []byte(`"Version":1`), 1),
		"alias-duplicate":         bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"Version":1`), 1),
		"surrogate":               bytes.Replace(raw, []byte(`"cursor":""`), []byte(`"cursor":"\ud800"`), 1),
		"low-surrogate":           bytes.Replace(raw, []byte(`"cursor":""`), []byte(`"cursor":"\udfff"`), 1),
		"invalid-utf8":            append(append([]byte{}, raw...), 0xff),
		"trailing":                append(append([]byte{}, raw...), []byte(` {}`)...),
		"request-bytes":           append(append([]byte{}, raw...), bytes.Repeat([]byte(" "), RequestBytes-len(raw)+1)...),
		"limit":                   bytes.Replace(raw, []byte(`"limit":0`), []byte(`"limit":101`), 1),
		"version":                 bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1),
		"response-limit":          bytes.Replace(raw, []byte(`"response_limit":0`), []byte(`"response_limit":1048577`), 1),
		"negative-response-limit": bytes.Replace(raw, []byte(`"response_limit":0`), []byte(`"response_limit":-1`), 1),
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			if _, e := DecodeRequest(b); e == nil {
				t.Fatal("invalid request admitted")
			}
		})
	}
	if _, e = DecodeRequest(append(raw, bytes.Repeat([]byte(" "), RequestBytes-len(raw))...)); e != nil {
		t.Fatal("exact request byte boundary", e)
	}
	q.Query = QuerySelector
	for _, name := range []string{"base64", "dot", "dotdot", "nul", "slash", "count", "component"} {
		t.Run(name, func(t *testing.T) {
			s := selector([]byte("a"))
			switch name {
			case "base64":
				s.Components[0] = "YR=="
			case "dot":
				s = selector([]byte("."))
			case "dotdot":
				s = selector([]byte(".."))
			case "nul":
				s = selector([]byte{0})
			case "slash":
				s = selector([]byte("a/b"))
			case "count":
				s.Components = make([]string, Depth+1)
				for i := range s.Components {
					s.Components[i] = "YQ=="
				}
			case "component":
				s = selector(bytes.Repeat([]byte("a"), 256))
			}
			q.Selector = &s
			b, _ := json.Marshal(q)
			if _, e := DecodeRequest(b); e == nil {
				t.Fatal("invalid selector admitted")
			}
		})
	}
	r := testReadResponse()
	r.Entries = make([]EntryDTO, 101)
	if _, e = EncodeResponse(r); e == nil {
		t.Fatal("response count overrun")
	}
	r = testReadResponse()
	raw, e = EncodeResponse(r)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = DecodeResponse(append(raw, bytes.Repeat([]byte(" "), ResponseBytes-len(raw))...)); e != nil {
		t.Fatal("exact response byte boundary", e)
	}
	if _, e = DecodeResponse(append(raw, bytes.Repeat([]byte(" "), ResponseBytes-len(raw)+1)...)); e == nil {
		t.Fatal("response byte overrun")
	}
}

func TestGenerationDTODecimal(t *testing.T) {
	for _, n := range []int64{math.MinInt64, -9007199254740993, -1, 0, 1, 9007199254740991, 9007199254740992, 9007199254740993, math.MaxInt64} {
		b, e := json.Marshal(Decimal(n))
		if e != nil || string(b) != strconv.Quote(strconv.FormatInt(n, 10)) {
			t.Fatal(n, e)
		}
		var got Decimal
		if json.Unmarshal(b, &got) != nil || int64(got) != n {
			t.Fatal(n)
		}
	}
	for _, s := range []string{`0`, `null`, `""`, `"01"`, `"-0"`, `"+1"`, `"1.0"`, `"1e3"`, `" 1"`, `"9223372036854775808"`, `"-9223372036854775809"`, `"\u0031"`} {
		t.Run(s, func(t *testing.T) {
			var n Decimal
			if json.Unmarshal([]byte(s), &n) == nil {
				t.Fatal("noncanonical int64")
			}
		})
	}
	r := testReadResponse()
	r.Entries[0].Metadata.MtimeNS = nil
	b, e := EncodeResponse(r)
	if e != nil {
		t.Fatal(e)
	}
	zero := Decimal(0)
	r.Entries[0].Metadata.MtimeNS = &zero
	other, e := EncodeResponse(r)
	if e != nil || bytes.Equal(b, other) {
		t.Fatal("unknown collapsed to zero")
	}
}

func TestGenerationDTOJSONConsumer(t *testing.T) {
	if os.Getenv("COLDKEEP_PHASE1C_TEST") != "1" {
		t.Skip("required in the staged recovery and complete codec routes")
	}
	version, e := exec.Command("node", "--version").Output()
	if e != nil || strings.TrimSpace(string(version)) != "v24.22.2" {
		t.Fatal("pinned Node 24.22.2 required", e)
	}
	for _, n := range []int64{math.MinInt64, -9007199254740993, -1, 0, 9007199254740991, 9007199254740992, 9007199254740993, math.MaxInt64} {
		r := testReadResponse()
		v := Decimal(n)
		id := Decimal(9007199254740993)
		r.Entries[0].LogicalFileID = &id
		r.Entries[0].Metadata.MtimeNS = &v
		b, e := EncodeResponse(r)
		if e != nil {
			t.Fatal(e)
		}
		cmd := exec.Command("node", "-e", `let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const x=JSON.parse(s),e=x.entries[0];if(typeof e.logical_file_id!=='string'||BigInt(e.logical_file_id).toString()!==e.logical_file_id||typeof e.metadata.mtime_ns!=='string'||BigInt(e.metadata.mtime_ns).toString()!==e.metadata.mtime_ns)process.exit(2);process.stdout.write(JSON.stringify(x));});`)
		cmd.Stdin = bytes.NewReader(b)
		out, e := cmd.Output()
		if e != nil {
			t.Fatal(e)
		}
		again, e := DecodeResponse(out)
		if e != nil || !reflect.DeepEqual(r, again) {
			t.Fatal("JavaScript lost integer identity", e)
		}
	}
}

func TestGenerationCursorCodec(t *testing.T) {
	c := Cursor{Version: 1, Query: QueryChildren, View: strings.Repeat("a", 32), RepositoryID: readIDs("repo"), SourceID: readIDs("source"), GenerationID: readIDs("generation"), Order: "raw-component-v1", Upper: base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0xff}, 255)), Last: "YQ==", EntryID: readIDs("entry")}
	raw, e := EncodeCursor(c)
	if e != nil {
		t.Fatal(e)
	}
	again, e := DecodeCursor(raw)
	if e != nil || !reflect.DeepEqual(c, again) {
		t.Fatal(e)
	}
	for _, s := range []string{raw + "=", strings.Repeat("a", CursorBytes+1), "!", base64.RawURLEncoding.EncodeToString([]byte(`{"version":1,"version":1}`))} {
		if _, e = DecodeCursor(s); e == nil {
			t.Fatal("malformed cursor accepted")
		}
	}
	for _, kind := range []string{"version", "order", "component", "entry", "watermark"} {
		t.Run(kind, func(t *testing.T) {
			bad := c
			switch kind {
			case "version":
				bad.Version++
			case "order":
				bad.Order = "display"
			case "component":
				bad.Last = "YR=="
			case "entry":
				bad.EntryID = "entry_unknown"
			case "watermark":
				bad.Upper = "YQ=="
				bad.Last = "Yg=="
			}
			b, _ := json.Marshal(bad)
			if _, e := DecodeCursor(base64.RawURLEncoding.EncodeToString(b)); e == nil {
				t.Fatal("invalid cursor admitted")
			}
		})
	}
}
