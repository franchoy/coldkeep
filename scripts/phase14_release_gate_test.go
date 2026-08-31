package scripts_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

var phase14SnapshotEvidenceNames = []string{
	"TestListRetainedLogicalFileIDs",
	"TestIsLogicalFileReferencedBySnapshot",
	"TestComputeReachabilitySummary",
	"TestRemoveFailsWhenLogicalFileIsRetainedBySnapshot",
	"TestRunGCDoesNotDeleteSnapshotRetainedContainer",
	"TestRunGCDryRunDoesNotCountSnapshotRetainedContainerAsReclaimable",
	"TestAdversarialG14SnapshotRetainedGCGuardUnderChurn",
	"TestDeleteSnapshotRemovesSnapshotRowsOnly",
	"TestAdversarialG17RetentionRootTransitionChurn",
	"TestRunStatsResultIncludesSnapshotRetentionVisibility",
	"TestRunStatsCommandJSONIncludesSnapshotRetention",
	"TestAdversarialG16SnapshotQueryContractChaos",
	"TestVerifySystemStandardPassesWithConsistentSnapshotReachability",
	"TestVerifySystemStandardDetectsOrphanSnapshotLogicalReference",
	"TestVerifySystemStandardDetectsSnapshotInvalidLifecycleState",
	"TestVerifySystemStandardDetectsSnapshotRetainedMissingChunkGraph",
	"TestFormatDoctorTextReportGoldenHealthy",
	"TestFormatDoctorTextReportGoldenDegraded",
	"TestAdversarialG15CorruptedSnapshotMetadataDetectionConservativeGC",
}

func TestSnapshotEvidenceNameValidatorUsesTrackedSourceOnly(t *testing.T) {
	script := filepath.Join(repoRoot(t), "scripts", "validate_snapshot_evidence_names.sh")

	t.Run("complete tracked source passes", func(t *testing.T) {
		repo := newPhase14GitRepo(t)
		writePhase14EvidenceSource(t, repo, phase14SnapshotEvidenceNames, true)
		runPhase14Command(t, repo, true, "bash", script, "--repo-root", repo)
	})

	t.Run("one missing name fails with diagnostic", func(t *testing.T) {
		repo := newPhase14GitRepo(t)
		missing := phase14SnapshotEvidenceNames[len(phase14SnapshotEvidenceNames)-1]
		writePhase14EvidenceSource(t, repo, phase14SnapshotEvidenceNames[:len(phase14SnapshotEvidenceNames)-1], true)
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo)
		if !strings.Contains(output, "missing evidence: "+missing) {
			t.Fatalf("missing-name diagnostic omitted %q:\n%s", missing, output)
		}
	})

	t.Run("untracked and ignored content cannot supply evidence", func(t *testing.T) {
		repo := newPhase14GitRepo(t)
		missing := phase14SnapshotEvidenceNames[len(phase14SnapshotEvidenceNames)-1]
		writePhase14EvidenceSource(t, repo, phase14SnapshotEvidenceNames[:len(phase14SnapshotEvidenceNames)-1], true)
		untracked := filepath.Join(repo, "runtime", "untracked.go")
		writePhase14File(t, untracked, "package fixture\nfunc "+missing+"() {}\n", 0o600)
		writePhase14File(t, filepath.Join(repo, ".gitignore"), "ignored-runtime/\n", 0o600)
		runPhase14Command(t, repo, true, "git", "add", ".gitignore")
		ignored := filepath.Join(repo, "ignored-runtime", "ignored.go")
		writePhase14File(t, ignored, "package fixture\nfunc "+missing+"() {}\n", 0o600)
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo)
		if !strings.Contains(output, "missing evidence: "+missing) {
			t.Fatalf("untracked evidence altered result:\n%s", output)
		}
	})

	t.Run("unreadable untracked and ignored directories are irrelevant", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("permission-mode proof is Unix-specific")
		}
		repo := newPhase14GitRepo(t)
		writePhase14EvidenceSource(t, repo, phase14SnapshotEvidenceNames, true)
		writePhase14File(t, filepath.Join(repo, ".gitignore"), "ignored-runtime/\n", 0o600)
		runPhase14Command(t, repo, true, "git", "add", ".gitignore")
		for _, name := range []string{"runtime", "ignored-runtime"} {
			dir := filepath.Join(repo, name)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatalf("create %s: %v", dir, err)
			}
			writePhase14File(t, filepath.Join(dir, "noise.go"), "not Go source", 0o600)
			if err := os.Chmod(dir, 0); err != nil {
				t.Fatalf("make %s unreadable: %v", dir, err)
			}
			t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		}
		runPhase14Command(t, repo, true, "bash", script, "--repo-root", repo)
	})

	t.Run("non git root fails", func(t *testing.T) {
		output := runPhase14Command(t, t.TempDir(), false, "bash", script, "--repo-root", t.TempDir())
		if !strings.Contains(output, "not a Git repository root") {
			t.Fatalf("non-git diagnostic missing:\n%s", output)
		}
	})
}

func TestBenchmarkEvidenceLifecycleLeavesCleanWorktree(t *testing.T) {
	script := filepath.Join(repoRoot(t), "scripts", "release_benchmark_evidence.sh")

	t.Run("valid exact SHA promotion is atomic and idempotent", func(t *testing.T) {
		repo, sha := newPhase14EvidenceRepo(t)
		bundle := newPhase14EvidenceBundle(t)
		preparePhase14Bundle(t, script, bundle, sha)
		runPhase14Command(t, repo, true, "bash", script, "promote", "--repo-root", repo, "--bundle-root", bundle, "--candidate-sha", sha)
		finalRoot := filepath.Join(repo, ".release-evidence", "v1.13.14", sha)
		if info, err := os.Stat(finalRoot); err != nil || !info.IsDir() {
			t.Fatalf("exact-SHA bundle missing at %s: %v", finalRoot, err)
		}
		manifestBefore := mustReadPhase14File(t, filepath.Join(finalRoot, "manifest.txt"))
		runPhase14Command(t, repo, true, "bash", script, "promote", "--repo-root", repo, "--bundle-root", bundle, "--candidate-sha", sha)
		manifestAfter := mustReadPhase14File(t, filepath.Join(finalRoot, "manifest.txt"))
		if manifestAfter != manifestBefore {
			t.Fatal("idempotent promotion changed the valid retained manifest")
		}
		runPhase14Command(t, repo, true, "bash", script, "inventory", "--repo-root", repo, "--require-clean-worktree")
		if output := runPhase14Command(t, repo, true, "git", "status", "--short", "--untracked-files=all"); strings.TrimSpace(output) != "" {
			t.Fatalf("ignored evidence lifecycle dirtied ordinary worktree:\n%s", output)
		}
		entries, err := os.ReadDir(filepath.Join(repo, ".release-evidence", "v1.13.14"))
		if err != nil {
			t.Fatalf("read retained evidence root: %v", err)
		}
		if len(entries) != 1 || entries[0].Name() != sha {
			t.Fatalf("unexpected retained inventory: %v", entries)
		}
	})

	t.Run("checksum mismatch and top manifest mismatch fail", func(t *testing.T) {
		_, sha := newPhase14EvidenceRepo(t)
		bundle := newPhase14EvidenceBundle(t)
		preparePhase14Bundle(t, script, bundle, sha)
		writePhase14File(t, filepath.Join(bundle, "profiles", "none-w1", "timing", "benchmark.json"), "corrupt\n", 0o600)
		output := runPhase14Command(t, bundle, false, "bash", script, "validate", "--bundle-root", bundle, "--candidate-sha", sha)
		if !strings.Contains(output, "checksum") {
			t.Fatalf("checksum mismatch diagnostic missing:\n%s", output)
		}

		bundle = newPhase14EvidenceBundle(t)
		preparePhase14Bundle(t, script, bundle, sha)
		manifestPath := filepath.Join(bundle, "manifest.txt")
		manifest := strings.Replace(mustReadPhase14File(t, manifestPath), "candidate_sha="+sha, "candidate_sha="+strings.Repeat("b", 40), 1)
		writePhase14File(t, manifestPath, manifest, 0o600)
		output = runPhase14Command(t, bundle, false, "bash", script, "validate", "--bundle-root", bundle, "--candidate-sha", sha)
		if !strings.Contains(output, "candidate SHA") {
			t.Fatalf("top-manifest mismatch diagnostic missing:\n%s", output)
		}
	})

	t.Run("missing profile fails", func(t *testing.T) {
		_, sha := newPhase14EvidenceRepo(t)
		bundle := newPhase14EvidenceBundle(t)
		if err := os.RemoveAll(filepath.Join(bundle, "profiles", "zstd-w4")); err != nil {
			t.Fatalf("remove profile fixture: %v", err)
		}
		output := runPhase14Command(t, bundle, false, "bash", script, "prepare", "--bundle-root", bundle, "--candidate-sha", sha, "--source-commit", sha, "--go-version", "go test", "--postgres-version", "postgres test", "--database-image-digest", "sha256:test")
		if !strings.Contains(output, "missing profile zstd-w4") {
			t.Fatalf("missing-profile diagnostic omitted:\n%s", output)
		}
	})

	t.Run("corrupt existing exact SHA is never overwritten", func(t *testing.T) {
		repo, sha := newPhase14EvidenceRepo(t)
		bundle := newPhase14EvidenceBundle(t)
		preparePhase14Bundle(t, script, bundle, sha)
		finalRoot := filepath.Join(repo, ".release-evidence", "v1.13.14", sha)
		writePhase14File(t, filepath.Join(finalRoot, "manifest.txt"), "corrupt\n", 0o600)
		output := runPhase14Command(t, repo, false, "bash", script, "promote", "--repo-root", repo, "--bundle-root", bundle, "--candidate-sha", sha)
		if !strings.Contains(output, "existing exact-SHA evidence is invalid") {
			t.Fatalf("corrupt-existing diagnostic missing:\n%s", output)
		}
		if got := mustReadPhase14File(t, filepath.Join(finalRoot, "manifest.txt")); got != "corrupt\n" {
			t.Fatalf("corrupt existing evidence was overwritten: %q", got)
		}
	})

	t.Run("unexpected incomplete staging fails inventory", func(t *testing.T) {
		repo, _ := newPhase14EvidenceRepo(t)
		staging := filepath.Join(repo, ".release-evidence", "v1.13.14", ".staging-stale")
		writePhase14File(t, filepath.Join(staging, "partial"), "incomplete\n", 0o600)
		output := runPhase14Command(t, repo, false, "bash", script, "inventory", "--repo-root", repo)
		if !strings.Contains(output, "unexpected staging directory remains") {
			t.Fatalf("stale-staging diagnostic missing:\n%s", output)
		}
		if err := os.RemoveAll(staging); err != nil {
			t.Fatalf("remove test-owned stale staging fixture: %v", err)
		}
		runPhase14Command(t, repo, true, "bash", script, "inventory", "--repo-root", repo, "--require-clean-worktree")
	})

	t.Run("signal cleans unique same-filesystem staging", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("signal and FIFO proof is Unix-specific")
		}
		if _, err := exec.LookPath("mkfifo"); err != nil {
			t.Skip("mkfifo is unavailable")
		}
		repo, sha := newPhase14EvidenceRepo(t)
		bundle := newPhase14EvidenceBundle(t)
		preparePhase14Bundle(t, script, bundle, sha)
		readyFIFO := filepath.Join(t.TempDir(), "ready.fifo")
		releaseFIFO := filepath.Join(t.TempDir(), "release.fifo")
		runPhase14Command(t, repo, true, "mkfifo", readyFIFO)
		runPhase14Command(t, repo, true, "mkfifo", releaseFIFO)
		cmd := exec.Command("bash", script, "promote", "--repo-root", repo, "--bundle-root", bundle, "--candidate-sha", sha)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(),
			"COLDKEEP_EVIDENCE_TEST_STAGE_READY_FIFO="+readyFIFO,
			"COLDKEEP_EVIDENCE_TEST_STAGE_RELEASE_FIFO="+releaseFIFO,
		)
		finalRoot := filepath.Join(repo, ".release-evidence", "v1.13.14", sha)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result, err := runPhase14SignalHarness(ctx, cmd, readyFIFO, releaseFIFO, "release\n", finalRoot)
		removePhase14TestFIFO(t, readyFIFO)
		removePhase14TestFIFO(t, releaseFIFO)
		if err != nil {
			t.Fatalf("bounded signal fixture: %v", err)
		}
		if result.ExitCode != 130 {
			t.Fatalf("signal fixture exit code = %d, want 130", result.ExitCode)
		}
		if result.ForcedKill || result.FallbackUsed {
			t.Fatalf("successful signal proof used failure cleanup: %+v", result)
		}
		if !result.ChildWaited || !result.ReadyReaderJoined || !result.ReleaseWriterJoined || !result.ReleaseWriterClosed {
			t.Fatalf("successful signal proof left helper ownership incomplete: %+v", result)
		}
		if _, err := os.Stat(result.Staging); !os.IsNotExist(err) {
			t.Fatalf("interrupted staging survived at %s: %v", result.Staging, err)
		}
		if _, err := os.Stat(finalRoot); !os.IsNotExist(err) {
			t.Fatalf("interrupted promotion exposed final SHA directory: %v", err)
		}
		t.Logf("benchmark signal cleanup evidence: pid=%d exit=130 staging=%s elapsed=%s", result.PID, result.Staging, result.Elapsed)
	})

	t.Run("transient profile roots are external and disjoint", func(t *testing.T) {
		runner := mustReadPhase14File(t, filepath.Join(repoRoot(t), "scripts", "run_release_benchmark_evidence.sh"))
		for _, required := range []string{"mktemp -d", `profiles/${profile}/integrity`, `profiles/${profile}/timing`} {
			if !strings.Contains(runner, required) {
				t.Fatalf("runner does not prove %q", required)
			}
		}
		if strings.Contains(runner, `retained_root="/.release-evidence`) || strings.Contains(runner, `work_root="/.release-evidence`) {
			t.Fatal("runner permits a filesystem-root evidence path")
		}
	})
}

func TestBenchmarkEvidenceSignalHarnessBoundsMissingReadiness(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("signal and FIFO proof is Unix-specific")
	}
	if _, err := exec.LookPath("mkfifo"); err != nil {
		t.Skip("mkfifo is unavailable")
	}

	root := t.TempDir()
	readyFIFO := filepath.Join(root, "ready.fifo")
	releaseFIFO := filepath.Join(root, "release.fifo")
	markerFIFO := filepath.Join(root, "blocked-child.fifo")
	for _, path := range []string{readyFIFO, releaseFIFO, markerFIFO} {
		runPhase14Command(t, root, true, "mkfifo", path)
	}

	cmd := exec.Command("bash", "-c", `read -r value < "$1"`, "phase14-missing-readiness", markerFIFO)
	cmd.Dir = root
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result, err := runPhase14SignalHarness(ctx, cmd, readyFIFO, releaseFIFO, "release\n", "")
	for _, path := range []string{readyFIFO, releaseFIFO, markerFIFO} {
		removePhase14TestFIFO(t, path)
	}

	if err == nil {
		t.Fatal("missing-readiness fixture unexpectedly succeeded")
	}
	var phaseErr *phase14SignalHarnessError
	if !errors.As(err, &phaseErr) || phaseErr.Phase != "readiness" {
		t.Fatalf("missing-readiness error did not identify readiness phase: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing-readiness error did not preserve deadline: %v", err)
	}
	if !result.ForcedKill || !result.FallbackUsed {
		t.Fatalf("missing-readiness fixture did not exercise bounded failure cleanup: %+v", result)
	}
	if !result.ChildWaited || !result.ReadyReaderJoined {
		t.Fatalf("missing-readiness fixture left child or reader ownership incomplete: %+v", result)
	}
	if result.ReleaseWriterStarted || result.ReleaseWriterJoined {
		t.Fatalf("missing-readiness fixture incorrectly entered release-writer phase: %+v", result)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatalf("inspect missing-readiness fixture root: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("missing-readiness fixture residue survived: %v", entries)
	}
	t.Logf("benchmark signal harness missing-readiness cleanup: phase=readiness deadline=500ms forced_kill=true fallback=true child_waited=true ready_joined=true elapsed=%s", result.Elapsed)
}

func TestReleaseLinearityValidatorAllowsInheritedAndRejectsLocalMerges(t *testing.T) {
	script := filepath.Join(repoRoot(t), "scripts", "validate_release_linearity.sh")

	t.Run("direct ancestry", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		commitPhase14(t, repo, "B")
		commitPhase14(t, repo, "C")
		setPhase14OriginMain(t, repo)
		runPhase14Command(t, repo, true, "git", "switch", "-c", "release/v1.13.14")
		commitPhase14(t, repo, "D")
		commitPhase14(t, repo, "E")
		runPhase14Command(t, repo, true, "bash", script, "--repo-root", repo)
	})

	t.Run("inherited main merge", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		commitPhase14(t, repo, "B")
		runPhase14Command(t, repo, true, "git", "switch", "-c", "topic")
		commitPhase14(t, repo, "topic")
		runPhase14Command(t, repo, true, "git", "switch", "main")
		runPhase14Command(t, repo, true, "git", "merge", "--no-ff", "topic", "-m", "M")
		commitPhase14(t, repo, "C")
		setPhase14OriginMain(t, repo)
		runPhase14Command(t, repo, true, "git", "switch", "-c", "release/v1.13.14")
		commitPhase14(t, repo, "R1")
		runPhase14Command(t, repo, true, "bash", script, "--repo-root", repo)
	})

	t.Run("main advance after release creation", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		commitPhase14(t, repo, "B")
		commitPhase14(t, repo, "C")
		runPhase14Command(t, repo, true, "git", "branch", "release/v1.13.14")
		commitPhase14(t, repo, "D")
		commitPhase14(t, repo, "E")
		setPhase14OriginMain(t, repo)
		runPhase14Command(t, repo, true, "git", "switch", "release/v1.13.14")
		commitPhase14(t, repo, "R1")
		commitPhase14(t, repo, "R2")
		runPhase14Command(t, repo, true, "bash", script, "--repo-root", repo)
	})

	t.Run("synthetic pull request merge validates authoritative release head", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		commitPhase14(t, repo, "B")
		commitPhase14(t, repo, "C")
		setPhase14OriginMain(t, repo)
		runPhase14Command(t, repo, true, "git", "switch", "-c", "release/v1.13.14")
		commitPhase14(t, repo, "R1")
		releaseSHA := commitPhase14(t, repo, "R2")
		runPhase14Command(t, repo, true, "git", "switch", "main")
		runPhase14Command(t, repo, true, "git", "merge", "--no-ff", "release/v1.13.14", "-m", "synthetic pull request merge")
		syntheticSHA := strings.TrimSpace(runPhase14Command(t, repo, true, "git", "rev-parse", "HEAD"))

		output := runPhase14Command(t, repo, true, "bash", script, "--repo-root", repo, "--candidate-ref", releaseSHA)
		if !strings.Contains(output, "RELEASE_LINEARITY_CANDIDATE_SHA: "+releaseSHA) {
			t.Fatalf("authoritative release candidate identity omitted:\n%s", output)
		}
		if strings.Contains(output, syntheticSHA) {
			t.Fatalf("synthetic merge SHA was treated as release lineage:\n%s", output)
		}

		defaultOutput := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo)
		if !strings.Contains(defaultOutput, syntheticSHA) || !strings.Contains(defaultOutput, "release-local merge commit") {
			t.Fatalf("default HEAD validation did not expose synthetic merge fixture %s:\n%s", syntheticSHA, defaultOutput)
		}
	})

	t.Run("release local merge is rejected with SHA", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		commitPhase14(t, repo, "B")
		runPhase14Command(t, repo, true, "git", "branch", "release/v1.13.14")
		commitPhase14(t, repo, "main-advance")
		setPhase14OriginMain(t, repo)
		runPhase14Command(t, repo, true, "git", "switch", "release/v1.13.14")
		commitPhase14(t, repo, "release-work")
		runPhase14Command(t, repo, true, "git", "merge", "--no-ff", "origin/main", "-m", "release local merge")
		mergeSHA := strings.TrimSpace(runPhase14Command(t, repo, true, "git", "rev-parse", "HEAD"))
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo)
		if !strings.Contains(output, mergeSHA) || !strings.Contains(output, "release-local merge commit") {
			t.Fatalf("local-merge diagnostic missing SHA %s:\n%s", mergeSHA, output)
		}
	})

	t.Run("missing origin main is rejected", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo)
		if !strings.Contains(output, "missing refs/remotes/origin/main") {
			t.Fatalf("missing-ref diagnostic omitted:\n%s", output)
		}
	})

	t.Run("unrelated history is rejected", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "main")
		setPhase14OriginMain(t, repo)
		runPhase14Command(t, repo, true, "git", "switch", "--orphan", "release/v1.13.14")
		commitPhase14(t, repo, "unrelated")
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo)
		if !strings.Contains(output, "no merge base") {
			t.Fatalf("unrelated-history diagnostic omitted:\n%s", output)
		}
	})

	t.Run("invalid explicit candidate is rejected", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		setPhase14OriginMain(t, repo)
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo, "--candidate-ref", "refs/heads/missing")
		if !strings.Contains(output, "candidate ref does not resolve to a commit") {
			t.Fatalf("invalid-candidate diagnostic omitted:\n%s", output)
		}
	})

	t.Run("missing explicit candidate argument is rejected", func(t *testing.T) {
		output := runPhase14Command(t, repoRoot(t), false, "bash", script, "--candidate-ref")
		if !strings.Contains(output, "--candidate-ref requires a commit or ref") {
			t.Fatalf("missing-candidate diagnostic omitted:\n%s", output)
		}
	})

	t.Run("non commit candidate object is rejected", func(t *testing.T) {
		repo := newPhase14GraphRepo(t)
		commitPhase14(t, repo, "A")
		setPhase14OriginMain(t, repo)
		objectPath := filepath.Join(repo, "blob.txt")
		writePhase14File(t, objectPath, "not a commit\n", 0o600)
		blobSHA := strings.TrimSpace(runPhase14Command(t, repo, true, "git", "hash-object", "-w", objectPath))
		output := runPhase14Command(t, repo, false, "bash", script, "--repo-root", repo, "--candidate-ref", blobSHA)
		if !strings.Contains(output, "candidate ref does not resolve to a commit") {
			t.Fatalf("non-commit diagnostic omitted:\n%s", output)
		}
	})
}

func newPhase14GitRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	runPhase14Command(t, repo, true, "git", "init", "-b", "main")
	runPhase14Command(t, repo, true, "git", "config", "user.name", "Phase 14 Test")
	runPhase14Command(t, repo, true, "git", "config", "user.email", "phase14@example.invalid")
	return repo
}

func newPhase14GraphRepo(t *testing.T) string {
	t.Helper()
	return newPhase14GitRepo(t)
}

func newPhase14EvidenceRepo(t *testing.T) (string, string) {
	t.Helper()
	repo := newPhase14GitRepo(t)
	writePhase14File(t, filepath.Join(repo, ".gitignore"), ".release-evidence/\n", 0o600)
	runPhase14Command(t, repo, true, "git", "add", ".gitignore")
	runPhase14Command(t, repo, true, "git", "commit", "-m", "fixture")
	sha := strings.TrimSpace(runPhase14Command(t, repo, true, "git", "rev-parse", "HEAD"))
	return repo, sha
}

func writePhase14EvidenceSource(t *testing.T, repo string, names []string, exact bool) {
	t.Helper()
	var source strings.Builder
	source.WriteString("package fixture\n")
	for _, name := range names {
		if exact {
			fmt.Fprintf(&source, "func %s() {}\n", name)
		} else {
			fmt.Fprintf(&source, "func  %s() {}\n", name)
		}
	}
	writePhase14File(t, filepath.Join(repo, "evidence.go"), source.String(), 0o600)
	runPhase14Command(t, repo, true, "git", "add", "evidence.go")
}

func newPhase14EvidenceBundle(t *testing.T) string {
	t.Helper()
	bundle := t.TempDir()
	for _, profile := range []string{"none-w1", "none-w4", "zstd-w1", "zstd-w4"} {
		integrity := filepath.Join(bundle, "profiles", profile, "integrity")
		timing := filepath.Join(bundle, "profiles", profile, "timing")
		writePhase14File(t, filepath.Join(integrity, "benchmark-integrity.json"), "{\"classification\":\"BENCHMARK_INTEGRITY_PASS\"}\n", 0o600)
		writePhase14Checksums(t, integrity, []string{"benchmark-integrity.json"})
		writePhase14File(t, filepath.Join(timing, "benchmark.json"), "{\"profile\":\""+profile+"\"}\n", 0o600)
		writePhase14File(t, filepath.Join(timing, "timing-advisory.json"), "{\"classification\":\"PASS\"}\n", 0o600)
		writePhase14Checksums(t, timing, []string{"benchmark.json", "timing-advisory.json"})
	}
	return bundle
}

func preparePhase14Bundle(t *testing.T, script, bundle, sha string) {
	t.Helper()
	runPhase14Command(t, bundle, true, "bash", script, "prepare", "--bundle-root", bundle, "--candidate-sha", sha, "--source-commit", sha, "--go-version", "go version test", "--postgres-version", "postgres test", "--database-image-digest", "sha256:test")
}

func writePhase14Checksums(t *testing.T, root string, names []string) {
	t.Helper()
	sort.Strings(names)
	var lines strings.Builder
	for _, name := range names {
		content := []byte(mustReadPhase14File(t, filepath.Join(root, name)))
		sum := sha256.Sum256(content)
		fmt.Fprintf(&lines, "%x  %s\n", sum, filepath.ToSlash(name))
	}
	writePhase14File(t, filepath.Join(root, "checksums.sha256"), lines.String(), 0o600)
}

func commitPhase14(t *testing.T, repo, label string) string {
	t.Helper()
	path := filepath.Join(repo, strings.ReplaceAll(label, " ", "-")+".txt")
	writePhase14File(t, path, label+"\n", 0o600)
	runPhase14Command(t, repo, true, "git", "add", filepath.Base(path))
	runPhase14Command(t, repo, true, "git", "commit", "-m", label)
	return strings.TrimSpace(runPhase14Command(t, repo, true, "git", "rev-parse", "HEAD"))
}

func setPhase14OriginMain(t *testing.T, repo string) {
	t.Helper()
	head := strings.TrimSpace(runPhase14Command(t, repo, true, "git", "rev-parse", "main"))
	runPhase14Command(t, repo, true, "git", "update-ref", "refs/remotes/origin/main", head)
}

func writePhase14File(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create parent for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustReadPhase14File(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

type phase14SignalHarnessResult struct {
	PID                   int
	ExitCode              int
	Staging               string
	Elapsed               time.Duration
	ChildWaited           bool
	ReadyReaderStarted    bool
	ReadyReaderJoined     bool
	ReleaseWriterStarted  bool
	ReleaseWriterJoined   bool
	ReleaseWriterClosed   bool
	ReleaseWriteTolerated bool
	FallbackUsed          bool
	ForcedKill            bool
}

type phase14SignalHarnessError struct {
	Phase string
	Err   error
}

func (e *phase14SignalHarnessError) Error() string {
	return fmt.Sprintf("benchmark signal harness %s phase: %v", e.Phase, e.Err)
}

func (e *phase14SignalHarnessError) Unwrap() error {
	return e.Err
}

type phase14ReadyReadResult struct {
	data []byte
	err  error
}

type phase14FIFOOpenResult struct {
	file *os.File
	err  error
}

func runPhase14SignalHarness(ctx context.Context, cmd *exec.Cmd, readyFIFO, releaseFIFO, releaseRecord, finalRoot string) (result phase14SignalHarnessResult, retErr error) {
	startedAt := time.Now()
	if !strings.HasSuffix(releaseRecord, "\n") {
		return result, &phase14SignalHarnessError{Phase: "setup", Err: errors.New("release record must end in a newline")}
	}
	if err := cmd.Start(); err != nil {
		result.Elapsed = time.Since(startedAt)
		return result, &phase14SignalHarnessError{Phase: "start", Err: err}
	}
	result.PID = cmd.Process.Pid

	var waitErr error
	waitDone := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(waitDone)
	}()

	var releaseWriter *os.File
	var readyResult <-chan phase14ReadyReadResult
	var readyDone <-chan struct{}
	var releaseResult <-chan phase14FIFOOpenResult
	var releaseDone <-chan struct{}
	readyResultConsumed := false
	releaseResultConsumed := false

	defer func() {
		result.Elapsed = time.Since(startedAt)
		if retErr == nil {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var cleanupProblems []string

		if releaseWriter != nil {
			if err := releaseWriter.Close(); err != nil {
				cleanupProblems = append(cleanupProblems, "close release writer: "+err.Error())
			}
			releaseWriter = nil
			result.ReleaseWriterClosed = true
		}

		if !result.ChildWaited {
			select {
			case <-waitDone:
				result.ChildWaited = true
			default:
			}
		}
		if !result.ChildWaited {
			result.ForcedKill = true
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				cleanupProblems = append(cleanupProblems, "kill exact child: "+err.Error())
			}
		}

		unblockFIFO := func(label, path string) {
			result.FallbackUsed = true
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				cleanupProblems = append(cleanupProblems, "fallback open "+label+": "+err.Error())
				return
			}
			if err := file.Close(); err != nil {
				cleanupProblems = append(cleanupProblems, "fallback close "+label+": "+err.Error())
			}
		}

		if result.ReadyReaderStarted && !result.ReadyReaderJoined {
			select {
			case <-readyDone:
				result.ReadyReaderJoined = true
			default:
				unblockFIFO("ready FIFO", readyFIFO)
			}
		}
		if result.ReadyReaderStarted && !result.ReadyReaderJoined {
			select {
			case <-readyDone:
				result.ReadyReaderJoined = true
			case <-cleanupCtx.Done():
				cleanupProblems = append(cleanupProblems, "join ready reader: "+cleanupCtx.Err().Error())
			}
		}
		if result.ReadyReaderJoined && !readyResultConsumed {
			select {
			case <-readyResult:
				readyResultConsumed = true
			default:
				cleanupProblems = append(cleanupProblems, "ready reader completed without a result")
			}
		}

		if result.ReleaseWriterStarted && !result.ReleaseWriterJoined {
			select {
			case <-releaseDone:
				result.ReleaseWriterJoined = true
			default:
				unblockFIFO("release FIFO", releaseFIFO)
			}
		}
		if result.ReleaseWriterStarted && !result.ReleaseWriterJoined {
			select {
			case <-releaseDone:
				result.ReleaseWriterJoined = true
			case <-cleanupCtx.Done():
				cleanupProblems = append(cleanupProblems, "join release writer: "+cleanupCtx.Err().Error())
			}
		}
		if result.ReleaseWriterJoined && !releaseResultConsumed {
			select {
			case opened := <-releaseResult:
				releaseResultConsumed = true
				if opened.file != nil {
					if err := opened.file.Close(); err != nil {
						cleanupProblems = append(cleanupProblems, "close cleanup release writer: "+err.Error())
					}
					result.ReleaseWriterClosed = true
				}
			default:
				cleanupProblems = append(cleanupProblems, "release writer completed without a result")
			}
		}

		if !result.ChildWaited {
			select {
			case <-waitDone:
				result.ChildWaited = true
			case <-cleanupCtx.Done():
				cleanupProblems = append(cleanupProblems, "join child wait owner: "+cleanupCtx.Err().Error())
			}
		}

		diagnostic := fmt.Sprintf("forced_kill=%t fallback=%t child_waited=%t ready_joined=%t release_joined=%t release_closed=%t",
			result.ForcedKill, result.FallbackUsed, result.ChildWaited, result.ReadyReaderJoined, result.ReleaseWriterJoined, result.ReleaseWriterClosed)
		if len(cleanupProblems) > 0 {
			diagnostic += " problems=" + strings.Join(cleanupProblems, "; ")
		}
		retErr = fmt.Errorf("%w; cleanup: %s", retErr, diagnostic)
	}()

	readyResults := make(chan phase14ReadyReadResult, 1)
	readyFinished := make(chan struct{})
	readyResult = readyResults
	readyDone = readyFinished
	result.ReadyReaderStarted = true
	go func() {
		data, err := os.ReadFile(readyFIFO)
		readyResults <- phase14ReadyReadResult{data: data, err: err}
		close(readyFinished)
	}()

	var ready phase14ReadyReadResult
	select {
	case ready = <-readyResults:
		readyResultConsumed = true
	case <-waitDone:
		result.ChildWaited = true
		return result, &phase14SignalHarnessError{Phase: "readiness", Err: fmt.Errorf("child exited before readiness: %w", waitErr)}
	case <-ctx.Done():
		return result, &phase14SignalHarnessError{Phase: "readiness", Err: ctx.Err()}
	}
	select {
	case <-readyFinished:
		result.ReadyReaderJoined = true
	case <-ctx.Done():
		return result, &phase14SignalHarnessError{Phase: "readiness", Err: fmt.Errorf("join reader: %w", ctx.Err())}
	}
	if ready.err != nil {
		return result, &phase14SignalHarnessError{Phase: "readiness", Err: ready.err}
	}
	result.Staging = strings.TrimSpace(string(ready.data))
	if result.Staging == "" {
		return result, &phase14SignalHarnessError{Phase: "readiness", Err: errors.New("promotion reported an empty staging path")}
	}
	select {
	case <-waitDone:
		result.ChildWaited = true
		return result, &phase14SignalHarnessError{Phase: "readiness", Err: fmt.Errorf("child exited after readiness: %w", waitErr)}
	default:
	}

	releaseResults := make(chan phase14FIFOOpenResult, 1)
	releaseFinished := make(chan struct{})
	releaseResult = releaseResults
	releaseDone = releaseFinished
	result.ReleaseWriterStarted = true
	go func() {
		file, err := os.OpenFile(releaseFIFO, os.O_WRONLY, 0)
		releaseResults <- phase14FIFOOpenResult{file: file, err: err}
		close(releaseFinished)
	}()

	var opened phase14FIFOOpenResult
	select {
	case opened = <-releaseResults:
		releaseResultConsumed = true
	case <-waitDone:
		result.ChildWaited = true
		return result, &phase14SignalHarnessError{Phase: "release-writer", Err: fmt.Errorf("child exited before writer establishment: %w", waitErr)}
	case <-ctx.Done():
		return result, &phase14SignalHarnessError{Phase: "release-writer", Err: ctx.Err()}
	}
	select {
	case <-releaseFinished:
		result.ReleaseWriterJoined = true
	case <-ctx.Done():
		return result, &phase14SignalHarnessError{Phase: "release-writer", Err: fmt.Errorf("join writer: %w", ctx.Err())}
	}
	if opened.err != nil {
		return result, &phase14SignalHarnessError{Phase: "release-writer", Err: opened.err}
	}
	releaseWriter = opened.file
	select {
	case <-waitDone:
		result.ChildWaited = true
		return result, &phase14SignalHarnessError{Phase: "release-writer", Err: fmt.Errorf("child exited after writer establishment: %w", waitErr)}
	default:
	}
	select {
	case <-ctx.Done():
		return result, &phase14SignalHarnessError{Phase: "signal", Err: ctx.Err()}
	default:
	}

	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		return result, &phase14SignalHarnessError{Phase: "signal", Err: err}
	}
	_, writeErr := releaseWriter.Write([]byte(releaseRecord))
	closeErr := releaseWriter.Close()
	releaseWriter = nil
	result.ReleaseWriterClosed = closeErr == nil
	if closeErr != nil {
		return result, &phase14SignalHarnessError{Phase: "release-write", Err: fmt.Errorf("close release writer: %w", closeErr)}
	}

	select {
	case <-waitDone:
		result.ChildWaited = true
	case <-ctx.Done():
		return result, &phase14SignalHarnessError{Phase: "child-exit", Err: ctx.Err()}
	}
	if waitErr == nil {
		return result, &phase14SignalHarnessError{Phase: "child-exit", Err: errors.New("interrupted promotion exited successfully")}
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		return result, &phase14SignalHarnessError{Phase: "child-exit", Err: fmt.Errorf("missing process-state exit evidence: %w", waitErr)}
	}
	result.ExitCode = exitErr.ExitCode()
	if result.ExitCode != 130 {
		return result, &phase14SignalHarnessError{Phase: "child-exit", Err: fmt.Errorf("exit code = %d, want 130", result.ExitCode)}
	}
	if writeErr != nil {
		// Exact exit 130 independently proves the interrupt result, so a release
		// write error is tolerable only after the Wait owner has reported it.
		result.ReleaseWriteTolerated = true
	}
	if _, err := os.Stat(result.Staging); !os.IsNotExist(err) {
		return result, &phase14SignalHarnessError{Phase: "assertion", Err: fmt.Errorf("staging survived at %s: %v", result.Staging, err)}
	}
	if finalRoot != "" {
		if _, err := os.Stat(finalRoot); !os.IsNotExist(err) {
			return result, &phase14SignalHarnessError{Phase: "assertion", Err: fmt.Errorf("final evidence root survived at %s: %v", finalRoot, err)}
		}
	}
	if !result.ChildWaited || !result.ReadyReaderJoined || !result.ReleaseWriterJoined || !result.ReleaseWriterClosed {
		return result, &phase14SignalHarnessError{Phase: "assertion", Err: errors.New("helper ownership did not fully join and close")}
	}
	if result.ForcedKill || result.FallbackUsed {
		return result, &phase14SignalHarnessError{Phase: "assertion", Err: errors.New("successful proof used failure cleanup")}
	}
	return result, nil
}

func removePhase14TestFIFO(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove test-owned FIFO %s: %v", path, err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("test-owned FIFO survived at %s: %v", path, err)
	}
}

func runPhase14Command(t *testing.T, dir string, wantSuccess bool, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	output, err := cmd.CombinedOutput()
	if wantSuccess && err != nil {
		t.Fatalf("command failed: %s %s\nerror: %v\noutput:\n%s", name, strings.Join(args, " "), err, output)
	}
	if !wantSuccess && err == nil {
		t.Fatalf("command unexpectedly succeeded: %s %s\noutput:\n%s", name, strings.Join(args, " "), output)
	}
	return string(output)
}
