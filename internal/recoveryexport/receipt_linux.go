package recoveryexport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
)

// ImportReceiptMaxObjects is bounded by the existing 20,000 catalog rows and
// directory entries, and 20,001 package members (one is the archive).
const ImportReceiptMaxObjects = 20000

// ImportReceiptMaxBytes preserves the admitted inventory capacity: Objects is
// already serialized inside the <=32 MiB diagnostic. The remaining receipt
// fields fit in 64 KiB: root and host each have <=4096 bytes (at most 6x JSON
// escaping), database/role <=63 each, key reference <=128, plus fixed strings,
// digests, field names and bounded integers. No product capacity is raised.
const ImportReceiptMaxBytes int64 = (32 << 20) + (64 << 10)

// ReadImportReceipt reads the complete regular, single-link receipt through the
// caller's payload-root anchor. The small operator-config limit is independent.
func ReadImportReceipt(ctx context.Context, root *os.Root) (receipt ImportReceipt, err error) {
	if ctx == nil || root == nil {
		return receipt, ErrConfiguration
	}
	if err = ctx.Err(); err != nil {
		return
	}
	f, e := openRegular(root, ".coldkeep-control/IMPORT_READY")
	if e != nil {
		return receipt, e
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	// copyBounded checks cancellation and actual bytes through physical EOF;
	// an artificial limit-reader EOF can never conceal trailing input.
	var b bytes.Buffer
	if _, err = copyBounded(ctx, &b, f, ImportReceiptMaxBytes, nil); err != nil {
		return
	}
	return decodeImportReceipt(ctx, b.Bytes())
}

func decodeImportReceipt(ctx context.Context, b []byte) (receipt ImportReceipt, err error) {
	if int64(len(b)) > ImportReceiptMaxBytes {
		return receipt, ErrLimit
	}
	// Token traversal applies depth/string/key limits and alias-aware Objects
	// counting before the typed decoder can allocate the object slice.
	err = strictJSON(ctx, b, &receipt, DefaultExportLimits().Members)
	return
}

func writeImportReceipt(ctx context.Context, control *os.Root, receipt *ImportReceipt, h *exportHooks) error {
	if len(receipt.Objects) > ImportReceiptMaxObjects {
		return ErrLimit
	}
	b, e := json.Marshal(receipt)
	if e != nil {
		return e
	}
	// Exercise exactly the consumer contract before even staging a marker.
	if _, e = decodeImportReceipt(ctx, b); e != nil {
		return e
	}
	return writeMember(control, "import-ready.pending", b, h)
}
