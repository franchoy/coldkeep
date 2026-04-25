package benchmark

import "bytes"

// Variant is one deterministic dataset input to feed through a chunker.
type Variant struct {
	Name string
	Data []byte
}

// Dataset groups a baseline input with realistic deterministic mutations.
type Dataset struct {
	Name      string
	Base      Variant
	Mutations []Variant
}

// Variants returns the baseline followed by all deterministic mutations.
func (d Dataset) Variants() []Variant {
	variants := make([]Variant, 0, 1+len(d.Mutations))
	variants = append(variants, d.Base)
	variants = append(variants, d.Mutations...)
	return variants
}

// DefaultDatasets returns the initial deterministic benchmark corpus for
// chunker validation and comparison work. The corpus is intentionally static so
// results are comparable over time and across machines.
func DefaultDatasets() []Dataset {
	return []Dataset{
		makeStructuredTextDataset(),
		makeBinaryAnchorDataset(),
		makeAppendHeavyDataset(),
	}
}

func makeStructuredTextDataset() Dataset {
	base := bytes.Repeat([]byte("chapter:intro\nsection:overview\ncontent:fastcdc-vs-v1\n\n"), 4096)
	insertedHeader := append(
		[]byte("chapter:preface\nsection:summary\ncontent:mutated-prefix\n\n"),
		base...,
	)
	replacedMiddle := cloneBytes(base)
	replacement := bytes.Repeat([]byte("content:mid-file-edited\n"), 256)
	replaceAt(replacedMiddle, len(replacedMiddle)/2, replacement)
	appendedTrailer := append(cloneBytes(base), bytes.Repeat([]byte("trailer:footer\n"), 768)...)

	return Dataset{
		Name: "structured-text",
		Base: Variant{Name: "base", Data: base},
		Mutations: []Variant{
			{Name: "inserted-header", Data: insertedHeader},
			{Name: "replaced-middle", Data: replacedMiddle},
			{Name: "appended-trailer", Data: appendedTrailer},
		},
	}
}

func makeBinaryAnchorDataset() Dataset {
	anchorA := bytes.Repeat([]byte{0x00, 0x11, 0x22, 0x33}, 16*1024)
	anchorB := bytes.Repeat([]byte{0xAA, 0xBB, 0xCC, 0xDD}, 16*1024)
	payload := bytes.Repeat([]byte("payload-block-"), 96*1024)
	base := append(append(cloneBytes(anchorA), payload...), anchorB...)

	insertedCenter := append(cloneBytes(base[:len(base)/2]), append(bytes.Repeat([]byte{0xFE, 0xED, 0xFA, 0xCE}, 8*1024), base[len(base)/2:]...)...)
	mutatedPrefix := cloneBytes(base)
	replaceAt(mutatedPrefix, 8*1024, bytes.Repeat([]byte{0x10, 0x20, 0x30, 0x40}, 4*1024))
	trimmedAndAppended := append(cloneBytes(base[12*1024:]), bytes.Repeat([]byte("suffix-anchor"), 32*1024)...)

	return Dataset{
		Name: "binary-anchor",
		Base: Variant{Name: "base", Data: base},
		Mutations: []Variant{
			{Name: "inserted-center", Data: insertedCenter},
			{Name: "mutated-prefix", Data: mutatedPrefix},
			{Name: "trimmed-appended", Data: trimmedAndAppended},
		},
	}
}

func makeAppendHeavyDataset() Dataset {
	segment := bytes.Repeat([]byte("record|field-a|field-b|field-c\n"), 8192)
	base := bytes.Repeat(segment, 12)
	appendedBurst := append(cloneBytes(base), bytes.Repeat([]byte("record|new-tail|mutation\n"), 12*1024)...)
	interleavedBurst := append(cloneBytes(base[:len(base)/3]), append(bytes.Repeat([]byte("record|interleave|mutation\n"), 6*1024), base[len(base)/3:]...)...)
	rewrittenTail := cloneBytes(base)
	replaceAt(rewrittenTail, len(rewrittenTail)-(512*1024), bytes.Repeat([]byte("record|rewritten-tail|mutation\n"), 8*1024))

	return Dataset{
		Name: "append-heavy",
		Base: Variant{Name: "base", Data: base},
		Mutations: []Variant{
			{Name: "appended-burst", Data: appendedBurst},
			{Name: "interleaved-burst", Data: interleavedBurst},
			{Name: "rewritten-tail", Data: rewrittenTail},
		},
	}
}

func cloneBytes(input []byte) []byte {
	return append([]byte(nil), input...)
}

func replaceAt(target []byte, start int, replacement []byte) {
	if start < 0 {
		start = 0
	}
	if start > len(target) {
		start = len(target)
	}
	end := start + len(replacement)
	if end > len(target) {
		end = len(target)
	}
	copy(target[start:end], replacement[:end-start])
}
