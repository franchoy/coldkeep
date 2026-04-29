package execution

import "testing"

func TestDefaultOptions(t *testing.T) {
	o := DefaultOptions()

	if o.StoreFolderWorkers != 1 {
		t.Fatalf("StoreFolderWorkers: got %d, want 1", o.StoreFolderWorkers)
	}
	if o.PipelineDepth != 1 {
		t.Fatalf("PipelineDepth: got %d, want 1", o.PipelineDepth)
	}
	if !o.Deterministic {
		t.Fatal("Deterministic: got false, want true")
	}
}

func TestOptionsNormalize(t *testing.T) {
	o := Options{
		StoreFolderWorkers: 0,
		PipelineDepth:      -10,
		Deterministic:      false,
	}.Normalize()

	if o.StoreFolderWorkers != 1 {
		t.Fatalf("StoreFolderWorkers: got %d, want 1", o.StoreFolderWorkers)
	}
	if o.PipelineDepth != 1 {
		t.Fatalf("PipelineDepth: got %d, want 1", o.PipelineDepth)
	}
	if o.Deterministic {
		t.Fatal("Deterministic should be preserved by Normalize")
	}
}

func TestOptionsValidate(t *testing.T) {
	valid := Options{StoreFolderWorkers: 1, PipelineDepth: 1, Deterministic: true}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate valid options: %v", err)
	}

	if err := (Options{StoreFolderWorkers: 0, PipelineDepth: 1}).Validate(); err == nil {
		t.Fatal("expected validation error for StoreFolderWorkers < 1")
	}

	if err := (Options{StoreFolderWorkers: 1, PipelineDepth: 0}).Validate(); err == nil {
		t.Fatal("expected validation error for PipelineDepth < 1")
	}
}

func TestNormalizeThenValidate(t *testing.T) {
	o := Options{StoreFolderWorkers: -2, PipelineDepth: 0, Deterministic: true}.Normalize()
	if err := o.Validate(); err != nil {
		t.Fatalf("Normalize then Validate: %v", err)
	}
}
