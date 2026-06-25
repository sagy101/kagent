# kagent AgentWorkflow — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `AgentWorkflow` CRD + controller + orchestrator runtime in a fork of `kagent-dev/kagent`, per `docs/proposals/kagent-EP-XXXX-agentworkflow-orchestration.md`, as a sequence of independently-shippable PRs.

**Architecture:** A new namespaced CRD (`kagent.dev/v1alpha2` `AgentWorkflow`) of **nodes** (refs to existing `Agent`s) + **edges** (v1: unconditional, single linear acyclic chain). A controller validates it and reconciles a lightweight **orchestrator runtime** (its own A2A-serving Deployment, registered in kagent's existing A2A mux via `SetAgentHandler` passthrough). At invocation the runtime walks the edges, calling each node's agent over A2A, threading a **context state bag** (tagged `DataPart`s) and, optionally, a **git workspace by reference** (commit SHA; agent-run git over its own egress; a repo-scoped credential surfaced per-invocation; a `verifyPush` readiness gate).

**Tech Stack:** Go (controller-runtime / kubebuilder), kagent `go/api/v1alpha2` + `go/core` controllers + A2A mux, `envtest` for controller tests, `kind` + `helm` for E2E. Each phase runs kagent's standard flow: `make generate manifests` → `make test` (unit + envtest) → `make e2e` (kind).

> **Working location:** a fork of `kagent-dev/kagent`, on a feature branch. File the tracking issue first to get the EP number (`XXXX`). Before coding, read the cited kagent files in the fork — exact API signatures (controller setup, the A2A mux interface, the ADK A2A client) are derived from the fork, not from this plan.

> **Conventions for every commit:** DCO sign-off (`git commit -s`). Keep `main` green. One phase = one PR.

---

## Reference map (read these in the fork before starting)

- `go/api/v1alpha2/agent_types.go` — CRD conventions: `TypedReference` (Kind/ApiGroup/Name/Namespace; only Name required), `GitAuthSecretRef *corev1.LocalObjectReference`, `SandboxConfig.Network.AllowedDomains` (`spec.sandbox.network`, deny-by-default), status = `metav1.Condition` + `ObservedGeneration`, marker style.
- `go/api/v1alpha2/groupversion_info.go` — `kagent.dev` / `v1alpha2` registration.
- `go/core/internal/controller/agent_controller.go` — reconciler pattern to mirror (status conditions, `SetupWithManager`).
- `go/core/internal/controller/sandboxagent_controller.go` — a **non-Agent** controller that registers into the A2A machinery (precedent for our orchestrator registration).
- `go/core/internal/a2a/a2a_handler_mux.go` — `SetAgentHandler(ref, *a2aclient.Client, card, mw)` / `RemoveAgentHandler`; passthrough-only (`NewPassthroughRequestHandler`).
- `go/core/internal/a2a/a2a_registrar.go` — how Agent/SandboxAgent endpoints are registered (`a2aclient.NewFromEndpoints`).
- `go/core/internal/httpserver/server.go` — `APIPathA2A = "/api/a2a"`, prefix `/{namespace}/{name}`.
- `go/adk/pkg/a2a/executor.go` — `sessionID := reqCtx.ContextID` (multi-turn within a contextId); the ADK A2A client we call nodes with.
- `go/adk/pkg/a2a/converter.go` + `consts.go` — `kagent_type`-tagged `DataPart` handling (untagged dropped); `A2ADataPartMetadataTypeKey`, `KAgentMetadataKeyPrefix`.
- `go/core/internal/skillsinit/git.go` — the existing `git clone` + `GitAuthSecretRef` → auth pattern (clone-only today); mirror its auth wiring for the per-invocation `.netrc`.
- `go/Makefile` + root `Makefile` — targets: `make generate manifests`, `make test`, `setup-envtest` / `ENVTEST_K8S_VERSION`, `make e2e` (runs `go test ./test/e2e` on a `kind` cluster), `KIND_*`, `helm install/uninstall`, `make check-api-key`.
- `go/core/test/e2e/` — existing E2E suite structure (invoke over A2A) to mirror.

---

## Phase 1 — CRD types + codegen

**Goal:** `AgentWorkflow` types compile, generate a CRD, install into a cluster, and reject malformed field values. No controller behaviour yet.

**Files:**
- Create: `go/api/v1alpha2/agentworkflow_types.go`
- Modify (codegen output): `go/api/v1alpha2/zz_generated.deepcopy.go`, `helm/kagent-crds/templates/*agentworkflow*.yaml`
- Test: `go/api/v1alpha2/agentworkflow_types_test.go`

- [ ] **Step 1: Write the types** — `go/api/v1alpha2/agentworkflow_types.go`, copied verbatim from the EP's "The `AgentWorkflow` CRD" section (the `AgentWorkflowSpec`/`AgentWorkflowNode`/`AgentWorkflowEdge`/`WorkspaceSpec`/`GitWorkspaceSource`/`WorkspaceHandoffPolicy`/`AgentWorkflowStatus` Go block, plus `WorkspaceAccessMode`). Add the standard kubebuilder root object scaffolding mirroring `agent_types.go`:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:categories=kagent
type AgentWorkflow struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   AgentWorkflowSpec   `json:"spec,omitempty"`
    Status AgentWorkflowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type AgentWorkflowList struct {
    metav1.TypeMeta `json:",inline"`
    metav1.ListMeta `json:"metadata,omitempty"`
    Items           []AgentWorkflow `json:"items"`
}

func init() { SchemeBuilder.Register(&AgentWorkflow{}, &AgentWorkflowList{}) }
```

Use `TypedReference` for `AgentWorkflowNode.AgentRef` (the existing kagent type — do **not** define a new ref type).

- [ ] **Step 2: Generate deepcopy + CRD manifests**

Run: `cd go && make generate manifests`
Expected: `zz_generated.deepcopy.go` gains `AgentWorkflow*` funcs; a new CRD yaml appears under `helm/kagent-crds/`. No errors.

- [ ] **Step 3: Write the failing CRD-validation test** — `agentworkflow_types_test.go` (envtest): apply a valid `AgentWorkflow` (1 node, 1 edge) and assert it's accepted; apply one with a node `name` of `Bad_Name` and assert the apiserver rejects it (pattern violation); apply one with `nodes: []` and assert rejection (MinItems).

```go
func TestAgentWorkflowCRDValidation(t *testing.T) {
    // envtest env with the generated CRDs installed (mirror an existing *_test.go in go/api or go/core)
    // valid: nodes:[{name: a, agentRef:{name: x}}], edges:[]  → Create succeeds
    // invalid name "Bad_Name" → Create returns an apiserver validation error
    // nodes: [] → Create returns MinItems error
}
```

- [ ] **Step 4: Run it — fails** (types/CRD not wired). Run: `cd go && make test ARGS="-run TestAgentWorkflowCRDValidation"` (or the repo's envtest invocation). Expected: FAIL.

- [ ] **Step 5: Make it pass** — ensure the CRD is in the envtest `CRDDirectoryPaths` (mirror existing setup), markers correct (`Pattern`, `MinItems`, `MaxItems`, `Enum`). Re-run Step 4. Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go/api/v1alpha2/agentworkflow_types.go go/api/v1alpha2/agentworkflow_types_test.go go/api/v1alpha2/zz_generated.deepcopy.go helm/kagent-crds/
git commit -s -m "feat(api): add AgentWorkflow CRD types (v1alpha2)"
```

**Acceptance:** CRD installs; valid spec accepted; bad name / empty nodes rejected by the apiserver.

---

## Phase 2 — Controller: reconcile validation + status

**Goal:** A reconciler validates an `AgentWorkflow` (edge graph is a single linear acyclic chain; every `agentRef` resolves; if a workspace is set, `gitAuthSecretRef` resolves and every `ReadOnly`/`ReadWrite` node's agent allows the repo host in `spec.sandbox.network`) and reports `Accepted` / `Ready` conditions. No orchestrator Deployment yet.

**Files:**
- Create: `go/core/internal/controller/agentworkflow_controller.go`, `go/core/internal/controller/graph.go` (pure graph helpers)
- Modify: the manager setup (where `agent_controller.go` is registered — `SetupWithManager`)
- Test: `go/core/internal/controller/graph_test.go`, `go/core/internal/controller/agentworkflow_controller_test.go` (envtest)

- [ ] **Step 1: Write the failing graph-validation unit test** — `graph_test.go`:

```go
func TestValidateLinearChain(t *testing.T) {
    cases := []struct{ name string; nodes []string; edges [][2]string; wantErr bool }{
        {"linear", []string{"a","b","c"}, [][2]string{{"a","b"},{"b","c"}}, false},
        {"single-node", []string{"a"}, nil, false},
        {"cycle", []string{"a","b"}, [][2]string{{"a","b"},{"b","a"}}, true},
        {"fork", []string{"a","b","c"}, [][2]string{{"a","b"},{"a","c"}}, true},
        {"two-entries", []string{"a","b","c"}, [][2]string{{"a","c"},{"b","c"}}, true},
        {"unknown-node", []string{"a","b"}, [][2]string{{"a","z"}}, true},
        {"disconnected", []string{"a","b","c"}, [][2]string{{"a","b"}}, true},
    }
    for _, c := range cases {
        t.Run(c.name, func(t *testing.T){
            err := ValidateLinearChain(c.nodes, c.edges)
            if (err != nil) != c.wantErr { t.Fatalf("got err=%v want %v", err, c.wantErr) }
        })
    }
}
```

- [ ] **Step 2: Run it — fails** (`ValidateLinearChain` undefined). Run: `cd go && go test ./core/internal/controller/ -run TestValidateLinearChain`. Expected: FAIL (undefined).

- [ ] **Step 3: Implement `ValidateLinearChain`** — `graph.go`: build adjacency from `edges`; error if any edge references an unknown node; require exactly one entry (in-degree 0), exactly one terminal (out-degree 0), every node in-degree ≤1 and out-degree ≤1, and a single path covering all nodes (walk from entry, count = len(nodes), no revisit → catches cycles, forks, two-entries, disconnected).

- [ ] **Step 4: Run it — passes.** Run as Step 2. Expected: PASS (all cases).

- [ ] **Step 5: Write the reconciler** — `agentworkflow_controller.go`, mirroring `agent_controller.go`: fetch the `AgentWorkflow`; call `ValidateLinearChain`; resolve each `node.agentRef` (Get the `Agent`); if `spec.workspace != nil`: Get the `gitAuthSecretRef` Secret, and for each `ReadOnly`/`ReadWrite` node confirm the resolved Agent's `spec.sandbox.network.allowedDomains` contains the `repository` host; set `Accepted=True`/`False` (with a message naming the offending node/edge) and `ObservedGeneration`. Register in `SetupWithManager`.

- [ ] **Step 6: Write the failing envtest** — `agentworkflow_controller_test.go`: apply a valid workflow → `Accepted=True`; apply a cyclic edge set → `Accepted=False` (reason mentions cycle); apply a node whose `agentRef` doesn't exist → `Accepted=False`; apply a `ReadWrite` node whose Agent lacks repo-host egress → `Accepted=False` naming the node.

- [ ] **Step 7: Run envtest — fails, then implement to green.** Run: `cd go && make test` (envtest subset). Iterate the reconciler until all four assertions pass.

- [ ] **Step 8: Commit**

```bash
git add go/core/internal/controller/agentworkflow_controller.go go/core/internal/controller/graph.go go/core/internal/controller/*_test.go <manager-setup-file>
git commit -s -m "feat(controller): AgentWorkflow reconcile + validation (graph, refs, egress) + status conditions"
```

**Acceptance:** `make test` green; conditions correct for valid/cyclic/missing-ref/missing-egress specs.

---

## Phase 3 — Orchestrator runtime (context plane) + Deployment + A2A registration

**Goal:** Reconcile creates an orchestrator Deployment/Service that **serves A2A**; the controller registers it in the mux; on invocation it walks the linear chain, threads the context state bag, and returns output. No workspace yet.

**Files:**
- Create: `go/core/cmd/agentworkflow/main.go` (orchestrator binary — mirror an existing kagent runtime cmd), `go/core/internal/orchestrator/runner.go` (edge walk + state bag), `go/core/internal/orchestrator/state.go` (state bag + `{key}` templating)
- Modify: `agentworkflow_controller.go` (create Deployment/Service; register via `SetAgentHandler` mirroring `sandboxagent_controller.go` + `a2a_registrar.go`); Dockerfile/helm image wiring
- Test: `go/core/internal/orchestrator/state_test.go`, `runner_test.go`; `go/core/test/e2e/agentworkflow_context_test.go`

- [ ] **Step 1: Write the failing state-bag test** — `state_test.go`:

```go
func TestStateBagTemplating(t *testing.T) {
    sb := NewStateBag()
    sb.Set("input", TextValue("classify this"))
    sb.Set("category", TextValue("bug"))
    got, err := sb.Render("Write a reply for a '{category}' issue about: {input}")
    if err != nil { t.Fatal(err) }
    want := "Write a reply for a 'bug' issue about: classify this"
    if got != want { t.Fatalf("got %q want %q", got, want) }
}

func TestStateBagUnknownKey(t *testing.T) {
    _, err := NewStateBag().Render("{missing}")
    if err == nil { t.Fatal("expected error for unknown key") }
}
```

- [ ] **Step 2: Run — fails.** `cd go && go test ./core/internal/orchestrator/ -run TestStateBag`. Expected: FAIL.

- [ ] **Step 3: Implement `state.go`** — a `StateBag` (map[string]Value; Value holds text + optional structured data from tagged `DataPart`s); `Set`, `Get`, and `Render(tmpl)` replacing `{key}` (error on unknown key; reserved key `input` cannot be an `outputKey` — enforce in the runner). Keep values addressable by key (field access `{key.field}` is a v2 extension — leave a `// v2:` note, do not implement).

- [ ] **Step 4: Run — passes.** As Step 2.

- [ ] **Step 5: Write the runner + its unit test** — `runner.go`: given a resolved workflow (ordered node list from the validated linear chain) + an A2A client factory, seed `input`, for each node build the input (`node.Input` rendered, or previous output if empty), `message/send` to the node's A2A endpoint with `contextId = runId`, capture text + tagged `DataPart`s, store under `outputKey` (default `node.Name`), and finally render `spec.output` (or terminal output). Unit-test with a **fake A2A client** (table: 2 nodes, assert call order, that node 2's input contains node 1's output, and the final output).

- [ ] **Step 6: Run runner unit test — green.** `cd go && go test ./core/internal/orchestrator/`.

- [ ] **Step 7: Wire the orchestrator binary + controller Deployment + mux registration** — `cmd/agentworkflow/main.go` serves A2A (mirror how an Agent pod serves A2A) and invokes `runner`. In the reconciler, create the Deployment+Service and call `SetAgentHandler` with an `a2aclient` pointed at the Service (mirror `a2a_registrar.go`). Add image build + helm wiring (mirror an existing runtime image).

- [ ] **Step 8: Write the failing E2E** — `go/core/test/e2e/agentworkflow_context_test.go`: on the kind cluster, apply two `Declarative` Agents (`triager`, `responder`) + a context-only `AgentWorkflow` (the EP's `triage-and-draft` example), invoke it over A2A at `/api/a2a/<ns>/triage-and-draft/`, assert the response reflects deterministic order and that `{category}` from node 1 reached node 2.

- [ ] **Step 9: Run E2E — green.** Run: `cd go && make e2e` (brings up kind + helm install; needs `make check-api-key`). Iterate until green. **Wrap with a hard timeout** (e.g. `gtimeout 1500 make e2e`) — never run unbounded.

- [ ] **Step 10: Commit**

```bash
git add go/core/cmd/agentworkflow/ go/core/internal/orchestrator/ go/core/internal/controller/agentworkflow_controller.go go/core/test/e2e/agentworkflow_context_test.go <docker/helm wiring>
git commit -s -m "feat(orchestrator): context-plane execution as an A2A endpoint (state bag + edge walk)"
```

**Acceptance:** `make test` + `make e2e` green; a context-only workflow runs deterministically over A2A with hand-off.

---

## Phase 4 — Workspace plane: git-by-reference + per-run branch + cred surfacing + egress (gate trusts the handoff)

**Goal:** A `ReadWrite` node receives workspace coordinates + a per-invocation credential, commits + pushes, and returns a handoff `DataPart`; a `ReadOnly` node checks out the ref. The orchestrator advances on the handoff (no independent verify yet — that's Phase 5).

**Files:**
- Modify: `runner.go` (per-run branch name; augment RO/RW node input with workspace coordinates + the handoff-contract instruction; parse the handoff `DataPart`; advance the current SHA), `agentworkflow_controller.go` (surface the `gitAuthSecretRef` to the node per-invocation — `.netrc`, mirroring `skillsinit/git.go` auth)
- Create: `go/core/internal/orchestrator/handoff.go` (handoff parse), `go/core/internal/orchestrator/workspace.go` (branch naming + coordinate message)
- Test: `handoff_test.go`, `workspace_test.go`; `go/core/test/e2e/agentworkflow_workspace_test.go`

- [ ] **Step 1: Failing handoff-parse test** — `handoff_test.go`:

```go
func TestParseHandoff(t *testing.T) {
    dp := dataPart(`{"committed":true,"pushed":true,"commit":"abc123","branch":"agentworkflow/w/run-1","summary":"x"}`)
    h, err := ParseHandoff([]DataPart{dp})
    if err != nil || !h.Committed || !h.Pushed || h.Commit != "abc123" {
        t.Fatalf("bad parse: %+v err=%v", h, err)
    }
}
func TestParseHandoffMissing(t *testing.T) {
    if _, err := ParseHandoff(nil); err == nil { t.Fatal("expected error when handoff absent") }
}
```

- [ ] **Step 2: Run — fails; implement `handoff.go`** — `Handoff{Committed, Pushed, Commit, Branch, Summary}`; `ParseHandoff(parts)` finds the `kagent_type`-tagged handoff `DataPart` and unmarshals; error if absent/invalid. Run test → green.

- [ ] **Step 3: Failing workspace-coordinates test** — `workspace_test.go`: assert `RunBranch("impl-review","run-7") == "agentworkflow/impl-review/run-7"`, and that `WorkspaceMessage(repo, branch, sha, ReadWrite)` includes the repo, branch, current SHA, access mode, and the commit+push+handoff instruction. Implement `workspace.go` → green.

- [ ] **Step 4: Extend the runner + cred surfacing** — in `runner.go`: when `spec.workspace != nil`, compute the run branch, and for each `ReadOnly`/`ReadWrite` node append the workspace coordinates (RW also gets the handoff instruction); after an RW node returns, `ParseHandoff` and set the current SHA = `handoff.commit` (trust for now). In the controller, surface `gitAuthSecretRef` to the node per-invocation as `.netrc` (mirror `skillsinit/git.go`'s auth construction). Unit-test the runner with the fake client (RW node input has coordinates+instruction; SHA advances from a stubbed handoff).

- [ ] **Step 5: Run unit — green.** `cd go && go test ./core/internal/orchestrator/`.

- [ ] **Step 6: Failing workspace E2E** — `agentworkflow_workspace_test.go`: a `BYO` coding Agent (`claude-coder`, RW) that makes a trivial change + commits + pushes + returns the handoff, then a `Declarative` reviewer (RO). Use a test git remote reachable from the kind cluster (an in-cluster gitea/http remote, or the e2e harness's fixture). Assert: the run branch exists with a commit chain, node 2 saw the pushed ref, and the workflow output includes the review. Ensure the BYO agent's `spec.sandbox.network` allows the remote host (else Phase 2 validation rejects it).

- [ ] **Step 7: Run E2E — green** (`gtimeout 1500 make e2e`). Iterate.

- [ ] **Step 8: Commit**

```bash
git add go/core/internal/orchestrator/handoff.go go/core/internal/orchestrator/workspace.go go/core/internal/orchestrator/runner.go go/core/internal/controller/agentworkflow_controller.go go/core/test/e2e/agentworkflow_workspace_test.go
git commit -s -m "feat(orchestrator): git-by-reference workspace — per-run branch, RO/RW, per-invocation cred surfacing"
```

**Acceptance:** an RW BYO node pushes (with the surfaced cred over its validated egress) and an RO node reads the advanced ref; E2E green.

---

## Phase 5 — Readiness gate hardening: verifyPush + re-prompt + maxRetries

**Goal:** Don't trust the handoff — independently confirm the pushed SHA, re-prompt on failure (bounded), and fail the node on exhaustion.

**Files:**
- Modify: `runner.go` (gate loop), `handoff.go` (verify)
- Create: `go/core/internal/orchestrator/verify.go` (`git ls-remote` check)
- Test: `verify_test.go`, extend `runner_test.go`; `go/core/test/e2e/agentworkflow_gate_test.go`

- [ ] **Step 1: Failing gate unit test** — extend `runner_test.go` with a fake A2A client + fake verifier:
  - handoff says pushed, verifier confirms SHA on branch → advance, 1 call.
  - handoff missing on first reply, present + verified on second → 2 calls (re-prompt), then advance.
  - never satisfied within `maxRetries` → node fails with a descriptive error.
  - `verifyPush:false` → trust the handoff without calling the verifier.

- [ ] **Step 2: Run — fails; implement the gate** — `verify.go`: `VerifyPush(repo, branch, sha)` via `git ls-remote <repo> <branch>` (reuse the `gitAuthSecretRef` auth) returning whether `sha` is the branch head/exists. In `runner.go`, wrap RW execution in a loop: send → ParseHandoff → (if `verifyPush`) VerifyPush → on success advance; on failure re-send a re-prompt message in the **same** `contextId` ("commit and push to `<branch>`, confirm the SHA"), bounded by `maxRetries`; on exhaustion mark the node failed (and the run failed) with a clear message. Run → green.

- [ ] **Step 3: Failing gate E2E** — `agentworkflow_gate_test.go`: (a) a misbehaving BYO node that returns `committed:true` but does **not** push → assert `verifyPush` rejects, the node is re-prompted, and after `maxRetries` the workflow fails with the gate message; (b) a node that pushes on the 2nd prompt → assert it advances.

- [ ] **Step 4: Run E2E — green** (`gtimeout 1500 make e2e`).

- [ ] **Step 5: Security test** — assert the surfaced credential never appears in the workflow's emitted A2A messages, node outputs, or controller/runtime logs (scan captured output for the token); assert a node cannot read another workflow's run branch (separate run IDs).

- [ ] **Step 6: Commit**

```bash
git add go/core/internal/orchestrator/verify.go go/core/internal/orchestrator/runner.go go/core/internal/orchestrator/*_test.go go/core/test/e2e/agentworkflow_gate_test.go
git commit -s -m "feat(orchestrator): handoff readiness gate (verifyPush + bounded re-prompt)"
```

**Acceptance:** spoofed handoff is rejected; bounded re-prompt works; exhaustion fails the run; credential never leaks.

---

## Phase 6 — Docs, sample, demo

**Goal:** Ship the accept-as-code evidence kagent expects (EP-1256 norm).

**Files:**
- Create: `docs/.../agent-workflow.md` (user docs mirroring an existing concept doc), `samples/agentworkflow/implement-and-review.yaml` (the EP example)
- Modify: any docs index/nav

- [ ] **Step 1: Write user docs** — what `AgentWorkflow` is, the `nodes`/`edges` model (v1 linear/unconditional), the context + workspace planes, the egress pre-req + repo-scoped cred, and a worked example. Link the EP.
- [ ] **Step 2: Add the sample** — the EP's `implement-and-review.yaml`, runnable on kind.
- [ ] **Step 3: Record a short demo** (asciinema/video) of the sample running end-to-end on kind — attach to the PR.
- [ ] **Step 4: Full-suite green** — `cd go && make test && gtimeout 1500 make e2e`. Expected: all green.
- [ ] **Step 5: Commit**

```bash
git add docs/ samples/agentworkflow/
git commit -s -m "docs: AgentWorkflow guide + sample + demo"
```

**Acceptance:** docs render, sample runs on kind, full suite green, demo attached.

---

## Test flow summary (every phase)

| Phase | Unit | envtest | E2E (kind) | Extra |
|---|---|---|---|---|
| 1 CRD types | marker validation | CRD install + field rejects | — | — |
| 2 Reconcile | graph validation | conditions (valid/cyclic/missing-ref/egress) | — | — |
| 3 Context runtime | state bag + runner | Deployment + mux registration | context-only workflow | — |
| 4 Workspace | handoff + workspace coords | cred surfacing | RW push + RO read | — |
| 5 Gate | verify + re-prompt loop | — | spoof-reject + bounded retry | security (cred non-leak, run isolation) |
| 6 Docs | — | — | full suite | demo |

Commands: `cd go && make generate manifests` · `make test` · `gtimeout 1500 make e2e` (kind; `make check-api-key` first). One phase = one PR, DCO-signed, `main` stays green.
