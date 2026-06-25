# EP-XXXX: AgentWorkflow — Declarative Orchestration of Standalone Agents over A2A

> **Draft.** `XXXX` is a placeholder — file the tracking issue, then rename to `EP-<issue#>-agentworkflow-orchestration.md` and place in `design/`.

* Issue: [#XXXX](https://github.com/kagent-dev/kagent/issues/XXXX) — _to be filed_

## Background

kagent today has only one way to involve several agents in a single flow, and it is not deterministic:

- **Agent-as-tool / supervisor** (`Tool.type: Agent`) — a coordinator `Agent` calls other deployed agents over A2A as tools. Composition is real, but **order and data flow are decided by the LLM at runtime** — non-deterministic by construction.

There is no declarative way to take **already-deployed agents, unchanged**, and run them in a **guaranteed order/graph** with **explicit data hand-off**, while **preserving each agent's own governance**. In particular, a graph node cannot today be a coding agent packaged as a **`BYO`** container (e.g. Claude Code) whose value is the **codebase it produces** — because A2A passes *messages*, not *filesystems*, and there is no first-class way to hand a worked-on codebase from one agent to the next.

This EP proposes `AgentWorkflow`: a new namespaced CRD + controller that orchestrates **references to existing `Agent` resources** over A2A, in a deterministic topology, with explicit context- and workspace-state transfer.

**In short:** an `AgentWorkflow` is a graph of **nodes** (each referencing an existing `Agent` — any kind: `Declarative`, `BYO`, `SandboxAgent`) connected by **edges**. A controller stands the workflow up as **its own A2A endpoint**; when invoked, it walks the edges, calling each node's agent over A2A in order, threading two kinds of state between them — **context**, via an orchestrator-held **state bag** (tagged `DataPart`s, referenced by key) and, optionally, a **git workspace passed by reference** (a commit SHA), where each write node commits + pushes and the run advances only once a verified **handoff** confirms it. **v1 edges are unconditional** (a single linear chain); conditional edges, loops, and parallel are v2 (see Forward-compatibility). Because the workflow is itself an A2A endpoint, it is composed and observed like any other agent.

## Motivation

Users want to compose a pipeline such as *"a Claude Code agent implements a change, a reviewer agent reviews it, a release agent opens a PR"* — where each node is an **independently-deployed, reusable** kagent `Agent` (potentially a **different harness** each), the nodes run in a **guaranteed order with explicit data hand-off** (not at a coordinator LLM's discretion), and the **codebase** one node produces is available to the next.

None of this is expressible in kagent today (see Background). The Goals below turn this into requirements.

### Goals

- Add a namespaced `AgentWorkflow` CRD: **nodes** (references to existing `Agent` resources) connected by **edges** — agents are used **as-is**, with no modification.
- Support **heterogeneous** nodes: any agent kind — `Declarative` (python/go), `BYO`, or `SandboxAgent` — reachable over A2A.
- **v1 topology:** a **single linear chain via unconditional edges** — deterministic, ordered. (Conditional/parallel/loops are v2; the edge model makes them additive.)
- **Context-state transfer:** an orchestrator-held state bag with per-node `outputKey` and `{key}` input templating, carried over A2A as `kagent_type`-tagged `DataPart`s. Outputs are **field-addressable** (`{key}` in v1, extensible to `{key.field}` in v2).
- **Workspace-state transfer:** a git-backed codebase passed **by reference** (commit SHA), with an agent-driven **handoff readiness gate** that confirms work is committed + pushed before the graph advances.
- An `AgentWorkflow` is itself **invocable over A2A** (like an `Agent`), so workflows are composable and observable through the same surface as everything else in kagent.
- Preserve each node agent's **own governance** (HITL, memory, network policy) by virtue of it being a normal, independently-deployed agent.

### Non-Goals

- **Conditional edges, loops, and parallel execution** — v2; designed to be additive (see Forward-compatibility).
- **Typed schema validation** of state-bag values — v2.
- Re-implementing the credential store — git creds reuse the `GitAuthSecretRef` Secret **pattern** kagent already uses (today for skills git-fetch); the EP adds only **per-run surfacing** of a repo-scoped token to a node (see Workspace). Egress remains the agent's own `spec.sandbox.network` (validated, not granted by the workflow).
- Non-git workspace transports (object-store / artifact-URI) — see Future Work.
- A visual authoring UI — out of scope for this EP (a separate concern).

## Implementation Details

### Why this design fits kagent

The design reuses kagent's existing extension points rather than adding parallel machinery:

- **CRD + controller is kagent's native extension unit** — `AgentWorkflow` is one more reconciled resource beside `Agent` / `RemoteMCPServer`, using the same `status`/conditions conventions.
- **A2A is the composition substrate.** The orchestrator drives nodes over A2A, and the workflow is *itself* an A2A endpoint — so it is callable, observable, and nestable through the surface kagent already exposes. No new transport, no new way to invoke it.
- **Agents are referenced, not redefined.** Each node is an existing `Agent`; its runtime/harness, HITL, memory and network policy come along unchanged. The workflow adds orchestration without forking the agent model.
- **It is additive.** A standalone, deterministic workflow over agent references is a capability kagent does not have today; adding it changes no existing resource or behaviour.
- **Reuse over reinvention throughout** — git auth via the existing `GitAuthSecretRef`, A2A tagged `DataPart`s for state, the ADK runtime, and (v2) the A2A interrupt transport.

### The `AgentWorkflow` CRD

A new top-level, namespaced kind under `kagent.dev/v1alpha2`. It is **not** a field on `Agent`: a workflow has no system message, model, memory or tools of its own — it is its own shape (a reviewer made exactly this point — that embedding a workflow under `DeclarativeAgentSpec` is the wrong shape — on the closed, unmerged PR [#1743](https://github.com/kagent-dev/kagent/pull/1743)), so it warrants its own kind rather than a field on the agent type.

The workflow is a graph of **nodes** (each an existing `Agent`) and **edges**. v1 edges are unconditional and must form a single linear, acyclic chain; v2 relaxes this additively (Forward-compatibility).

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:resource:categories=kagent

// +kubebuilder:validation:Enum=None;ReadOnly;ReadWrite
type WorkspaceAccessMode string

const (
    WorkspaceAccessNone      WorkspaceAccessMode = "None"
    WorkspaceAccessReadOnly  WorkspaceAccessMode = "ReadOnly"
    WorkspaceAccessReadWrite WorkspaceAccessMode = "ReadWrite"
)

type AgentWorkflowSpec struct {
    // Nodes are the graph's nodes; each references an existing Agent.
    // +kubebuilder:validation:MinItems=1
    // +kubebuilder:validation:MaxItems=20
    Nodes []AgentWorkflowNode `json:"nodes"`

    // Edges define execution order: {from, to} runs `to` after `from`.
    // v1: edges are UNCONDITIONAL and must form a single linear, acyclic chain
    // (exactly one entry node with no inbound edge, one terminal with none outbound).
    // v2 adds When (condition), back-edges (loops), and fan-out/in (parallel).
    // +kubebuilder:validation:MaxItems=40
    Edges []AgentWorkflowEdge `json:"edges"`

    // Workspace optionally declares a git-backed codebase shared across nodes,
    // passed by reference. Omit for context-only workflows.
    // +optional
    Workspace *WorkspaceSpec `json:"workspace,omitempty"`

    // Output is an optional template producing the workflow's final result from
    // the accumulated state bag (e.g. "{review}"). Defaults to the terminal node's output.
    // +optional
    Output string `json:"output,omitempty"`
}

type AgentWorkflowNode struct {
    // Name uniquely identifies the node, is its default OutputKey, and is referenced by edges.
    // +kubebuilder:validation:Pattern=`^[a-z]([-a-z0-9]*[a-z0-9])?$`
    Name string `json:"name"`

    // AgentRef references an existing Agent via kagent's TypedReference — the same
    // type agent-as-tool (Tool.Agent) uses. Any kind (Declarative, BYO, SandboxAgent),
    // invoked over A2A, unchanged.
    // +kubebuilder:validation:Required
    AgentRef TypedReference `json:"agentRef"`

    // Input templates this node's input message. "{key}" placeholders interpolate from
    // the state bag (reserved key "{input}" = the workflow's invocation message). If
    // omitted, the predecessor node's output is used.
    // +optional
    Input string `json:"input,omitempty"`

    // OutputKey is the state-bag key for this node's output. Defaults to Name.
    // +optional
    OutputKey string `json:"outputKey,omitempty"`

    // Workspace selects this node's access to the shared codebase.
    // +kubebuilder:default=None
    Workspace WorkspaceAccessMode `json:"workspace,omitempty"`
}

type AgentWorkflowEdge struct {
    // From and To are node Names. v1 edges are unconditional.
    // (v2 adds an optional When condition on a {node.field}, and back-edges for loops.)
    // +kubebuilder:validation:Required
    From string `json:"from"`
    // +kubebuilder:validation:Required
    To string `json:"to"`
}

// AgentRef (and any cross-resource reference) reuses kagent's existing TypedReference
// (Kind/ApiGroup/Name/Namespace) — the type Tool.Agent already uses — or
// TypedLocalReference for same-namespace refs. No bespoke reference type.

type WorkspaceSpec struct {
    Git     GitWorkspaceSource     `json:"git"`
    // +optional
    Handoff WorkspaceHandoffPolicy `json:"handoff,omitempty"`
}

type GitWorkspaceSource struct {
    // Repository is the clone URL.
    // +kubebuilder:validation:Required
    Repository string `json:"repository"`
    // BaseRef is the branch/tag/SHA the first ReadWrite node starts from.
    // +kubebuilder:default=main
    BaseRef string `json:"baseRef,omitempty"`
    // GitAuthSecretRef is a repo-scoped git Secret (token / ssh key), following the same
    // GitAuthSecretRef pattern kagent already uses for skills git-fetch (spec.skills). The orchestrator
    // surfaces it to each ReadOnly/ReadWrite node per-invocation (harness .netrc); the
    // agent never stores it. Egress to the repo host is a separate, validated pre-req
    // (the agent's spec.sandbox.network), not injected. Required if any node is ReadOnly/ReadWrite.
    // +optional
    GitAuthSecretRef *corev1.LocalObjectReference `json:"gitAuthSecretRef,omitempty"`
}

type WorkspaceHandoffPolicy struct {
    // MaxRetries bounds how many times the orchestrator re-prompts a ReadWrite node
    // to commit+push before failing the node.
    // +kubebuilder:default=2
    MaxRetries int32 `json:"maxRetries,omitempty"`
    // VerifyPush independently confirms the reported commit SHA exists on the run
    // branch (git ls-remote) before advancing. Recommended.
    // +kubebuilder:default=true
    VerifyPush bool `json:"verifyPush,omitempty"`
}

type AgentWorkflowStatus struct {
    // +optional
    Conditions []metav1.Condition `json:"conditions,omitempty"`
    // +optional
    ObservedGeneration int64 `json:"observedGeneration,omitempty"`
    // A2AEndpoint is the resolved A2A address at which this workflow is invocable.
    // +optional
    A2AEndpoint string `json:"a2aEndpoint,omitempty"`
}
```

Example (`context` + `workspace`):

```yaml
apiVersion: kagent.dev/v1alpha2
kind: AgentWorkflow
metadata:
  name: implement-and-review
  namespace: kagent
spec:
  workspace:
    git:
      repository: https://github.com/acme/widget.git
      baseRef: main
      gitAuthSecretRef: { name: git-bot-token }   # repo-scoped; surfaced per-invocation
    handoff: { maxRetries: 2, verifyPush: true }
  nodes:
    - name: implement
      agentRef: { name: claude-coder }     # a BYO Claude Code container (serves A2A)
      input: "Implement the feature described here:\n{input}"
      workspace: ReadWrite                  # mutates code → handoff gate enforced
      outputKey: implementation
    - name: review
      agentRef: { name: code-reviewer }     # a Declarative ADK agent
      input: "Review the changes on the workflow branch; summarize risks."
      workspace: ReadOnly                   # checks out the ref, no commit/push, no gate
      outputKey: review
  edges:
    - { from: implement, to: review }       # v1: unconditional, linear
  output: "{review}"
# Invocable over A2A like any agent:  POST /api/a2a/kagent/implement-and-review/
```

### Workflow-as-an-A2A-endpoint (controller + runtime)

The `AgentWorkflow` controller reconciles each resource into a lightweight **orchestrator runtime** — its own Deployment + Service that **serves A2A** (like an agent pod) and walks the node graph. The controller registers it in kagent's **existing A2A mux** exactly as `Agent`s and `SandboxAgent`s are registered: the mux holds a passthrough to the backend's A2A endpoint (`SetAgentHandler`), so routing a third resource type needs **no mux change**. The orchestration loop lives in the orchestrator runtime itself — it does **not** reuse the per-`Agent` ADK executor. RBAC grants read of the referenced `Agent`s. The workflow thus **exposes its own A2A endpoint** (`…/api/a2a/{namespace}/{name}` on the kagent core HTTP server) and behaves like an agent to callers — composable (a node in another workflow, or an agent-as-tool) and observable through kagent's existing A2A surface.

On reconcile the controller validates that the edge set forms a single linear acyclic chain, that every `agentRef` resolves, that (if a workspace is declared) `gitAuthSecretRef` resolves, and that every `ReadOnly`/`ReadWrite` node's agent has the repo host in its `spec.sandbox.network` egress; it publishes `status.a2aEndpoint` and standard `Accepted`/`Ready` conditions with `observedGeneration`.

### Execution model

On an A2A invocation the orchestrator runtime:

1. Seeds the **state bag** with the reserved key `input` = the invocation message.
2. If a workspace is declared, creates a **single per-run branch** `agentworkflow/<name>/<runId>` from `baseRef`. All nodes in the run share this one branch and build on each other as a **linear commit chain** (a node checks out the branch HEAD produced by its predecessor). It is one branch per *run*, **not** per node. The per-run branch isolates the run from `baseRef` and from other concurrent runs, and yields an auditable commit chain.
3. Starting at the **entry node** (no inbound edge), and following edges to the terminal node, for each node:
   1. Build the input message from `node.input` (interpolating `{key}` from the state bag); if `input` is omitted, use the predecessor's output.
   2. If `workspace` is `ReadOnly`/`ReadWrite`, include the workspace coordinates (repository, run branch, current SHA) and access mode; for `ReadWrite`, append the **handoff-contract** instruction.
   3. A2A `message/send` to the node's agent endpoint with a stable `contextId = runId`.
   4. For `ReadWrite` nodes, apply the **readiness gate** (below).
   5. Store the node result (text + `DataPart`s) under `outputKey` (default `Name`).
4. Produce the final result from `spec.output` (or the terminal node's output) and return it as the A2A response.

Multi-turn within one `contextId` resuming the same session is how kagent's A2A executor already works (`go/adk/pkg/a2a/executor.go`), so the re-prompt loops below rest on existing behaviour.

### State transfer — context plane

The orchestrator holds the state bag (it is **not** any agent's `session.state`). This is deliberately harness-agnostic: because the orchestrator marshals state via A2A message parts, it works identically for `Declarative`, `BYO`, and `SandboxAgent` nodes. Each node's output is captured from the A2A **text part** (and structured `DataPart`s) and stored under `outputKey`; `{key}` placeholders in a later node's `input` interpolate from the bag — a node may reference **any** earlier output. Outputs are stored **field-addressable** (v1 references the whole value as `{key}`; v2 extends to `{key.field}` for conditions/loops — no shape change).

Note: kagent's A2A converter drops **untagged** `DataPart`s, and the round-trip is asymmetric (the outbound path keeps text parts), so structured state travels in **`kagent_type`-tagged `DataPart`s** (or as text). Registering the workflow's state `kagent_type` is a small converter addition included with this EP. We do **not** rely on `FilePart`/`FileWithUri` for pass-by-reference — the commit SHA travels as a small tagged value, not files.

### State transfer — workspace plane (git-by-reference, agent-run, gated)

**The workspace plane is entirely opt-in.** Omit `spec.workspace` and the workflow is plain A2A orchestration with the context plane only — no git, no handoff, no gate. Even with a workspace declared, each node's `workspace` access **defaults to `None`**; the **handoff readiness gate applies only to `ReadWrite` nodes**. A user who "just wants normal A2A" simply declares no workspace (see the context-only example below).

```yaml
# Context-only workflow — no workspace transfer, plain A2A orchestration
apiVersion: kagent.dev/v1alpha2
kind: AgentWorkflow
metadata: { name: triage-and-draft, namespace: kagent }
spec:
  nodes:
    - name: triage
      agentRef: { name: triager }
      input: "Classify this issue:\n{input}"
      outputKey: category
    - name: draft
      agentRef: { name: responder }
      input: "Write a reply for a '{category}' issue."
  edges:
    - { from: triage, to: draft }
  output: "{draft}"
```

A codebase is passed **by reference**: the bytes live in the git remote and only the **commit SHA** crosses A2A (a small value in the state bag) — the codebase never travels over A2A. Bytes move **agent ↔ remote directly**, over the agent's own egress.

**Git is agent-run, not a kagent tool.** `commit` and `push` are ordinary shell commands a coding node already runs (`git commit` is local; `git push` is the same exec tool + network), so kagent needs no dedicated push tool. A `ReadOnly` node clones/fetches; a `ReadWrite` node also commits + pushes; `None` nodes touch no git.

A participating (`ReadOnly`/`ReadWrite`) node's agent needs two things, handled differently because of how kagent scopes each:

- **Egress to the repo host** — deploy-time per-agent policy (`spec.sandbox.network` allow-list) that **cannot** be granted to a referenced agent at run time. So the controller **validates** it at reconcile and **fails fast** (`Accepted=False`, naming the node) if a participating agent lacks it — never a silent mid-run break. It is a documented pre-requisite, not injected.
- **The git credential** — the workflow holds a **repo-scoped `GitAuthSecretRef`** and **surfaces it to the node per-invocation** (the harness writes `.netrc` for that run; the agent never stores it). This per-run surfacing is the **one small new mechanism** this EP adds — cred-per-invocation is feasible, unlike egress.

**Scoping.** The token is **repo-scoped** (a fine-grained PAT / deploy key / App-installation token), so a node can touch only that repository; **branch** scoping is enforced on the remote (branch-protection / push rules) plus the per-run branch convention — e.g. allow pushes only to `agentworkflow/<name>/*`. (Egress allow-listing is host-level only.)

Each `ReadWrite` node must return a structured **handoff** as an A2A `DataPart`:

```json
{ "committed": true, "pushed": true, "commit": "<sha>",
  "branch": "agentworkflow/<name>/<runId>", "summary": "…" }
```

This is a **fixed minimal schema**. Where the agent runtime supports structured output, the orchestrator requests the handoff natively; otherwise it is elicited by prompt convention and parsed from the response.

**Readiness gate** (per `ReadWrite` node):
1. If the handoff is present and `committed && pushed && commit` is set → candidate to advance.
2. If `verifyPush` (default), the orchestrator independently confirms the SHA exists on the run branch via `git ls-remote` — closing the "agent claimed but didn't push" gap.
3. If not ready (missing/false handoff, or verification fails), **re-prompt the same agent in the same A2A session** (`contextId`): *"Your work isn't committed/pushed; commit and push to `<branch>` and confirm with the SHA."* — bounded by `maxRetries`.
4. On exhaustion, fail the node (and the workflow) with a descriptive condition.

Re-prompting is native to A2A (multi-turn within a Task/`contextId`), so the gate needs no special transport. `ReadOnly` nodes check out the current SHA and run with no gate; `None` nodes are pure context-plane.

### Dependencies

- **Git:** a `ReadOnly`/`ReadWrite` node runs git itself over its **pre-configured egress** to the repo host (the agent's `spec.sandbox.network` — **validated, not injected**). The workflow holds a **repo-scoped `GitAuthSecretRef`** and **surfaces it per-invocation** (harness `.netrc`) — the one small new mechanism here; clone, the git binary, and egress are existing sandbox capability. Branch scoping is enforced via remote push-rules + the per-run branch.
- The reserved state key `input`, and the `DataPart`-based handoff schema, are runtime contracts owned by the orchestrator.

### Versioning, compatibility & limits

- **Additive, non-breaking.** `AgentWorkflow` is a **new kind added under the existing `kagent.dev/v1alpha2`** group/version. It introduces no change to the `Agent` CRD or any existing resource — adopting it is purely opt-in.
- **Safe default.** The workspace plane is **off by default** (omit `spec.workspace`; nodes default to `workspace: None`), so a workflow with no workspace is plain A2A orchestration with no new failure surface.
- **Bounds & defaults.** `nodes` capped (`MaxItems=20`), `edges` (`MaxItems=40`); each node A2A call carries a per-node timeout (default `300s`); state-bag values are size-bounded (default cap `256KiB` per key; larger payloads must be passed by reference); reserved keys (`input`) may not be used as an `outputKey`.

### Forward-compatibility (v2)

v1 deliberately models the workflow as **nodes + edges** — even though v1 only uses the simplest case (a single unconditional linear chain) — so the v2 roadmap is **purely additive**: new optional fields and relaxed validation, never a reshape.

| v2 feature | v1 constraint it relaxes | Additive change |
|---|---|---|
| **Conditional branching** | edges are unconditional | add optional `when` (a condition on `{node.field}`) to an edge; allow multiple out-edges from a node |
| **Parallel** | the chain is a single linear path | allow fan-out/join; execute independent branches concurrently |
| **Loop** | the graph is acyclic | allow a back-edge + `maxIterations` + an until-`when` |
| **Typed state** | state values are untyped | add an optional output schema per node (`runtime.RawExtension`); validate (orchestrator-side JSON Schema) |

All four read/condition against the **state bag**, which v1 already populates with structured, **field-addressable** outputs (`{node}` → `{node.field}`). So the data v2 needs is captured from day one; v2 adds only the condition evaluator + the relaxed edge rules. The `nodes` / `edges` / state-bag shapes do not change. (Loop and conditional share one primitive — condition evaluation on a state field; parallel is orthogonal — concurrency.)

### Test Plan

This mirrors kagent's existing test structure (CONTRIBUTING → Unit + E2E; controllers via `envtest`; E2E on `kind`):

- **Unit:** CRD validation (node/edge shape, linear-acyclic check, enum/pattern/min-items); input templating + state-bag resolution; default linear hand-off; handoff parsing + readiness-gate logic; `verifyPush` success/failure paths; output rendering.
- **Integration (`envtest`):** controller reconcile → orchestrator Deployment/Service + A2A-mux handler registered; bad edge sets, unresolved `agentRef`/`gitAuthSecretRef`, and missing node egress surface as `Accepted=False` conditions; `status.a2aEndpoint` published.
- **E2E (`kind` + `helm install`):** an `AgentWorkflow` of two **heterogeneous** agents (a `BYO` Claude Code `ReadWrite` node + a `Declarative` `ReadOnly` node) invoked over A2A — asserts edge order, context hand-off via tagged `DataPart`, git ref advancing across nodes, the readiness gate re-prompting on a missing handoff, and failure when the gate is exhausted.
- **Security:** the git credential never appears in any node's input/output/logs; a spoofed handoff is rejected by `verifyPush`; per-run branches isolate concurrent runs; a node cannot read another workflow's run branch.

Per kagent's accept-as-code norm (EP-1256), the implementation PR lands these as **real** unit/handler/e2e tests plus a short demo — not stub/no-op fakes.

## Alternatives

- **Ordered node list (no edges).** Simpler for v1, but v2's conditional/parallel/loop routing would have to be bolted on as a second source of truth (array order vs. explicit routing) — a reshape. Modelling edges from v1 makes v2 additive (Forward-compatibility), at a small cost to v1 verbosity.
- **Agent-as-tool / supervisor (exists).** Rejected as the primitive: LLM-routed, non-deterministic order and data flow.
- **Bundle the sequence inside one agent** (e.g. a `BYO` agent that orchestrates internally). Rejected: nodes are then not independently deployed/reusable, must share one runtime (no mixing a sandboxed coding node with a `Declarative` node), and orchestration is opaque to kagent (no per-node status, no reuse).
- **Move the workspace over A2A** (e.g. a git bundle as a message). Rejected: routes bytes through A2A. Pass-by-reference (bytes in the remote, SHA on A2A) keeps A2A small.
- **Shared workspace PVC across nodes.** Rejected for v1: a shared mount fights the sandbox/substrate isolation model and is unsafe for future parallel nodes. Git-by-reference is isolation-friendly.
- **Shared ADK session/`session.state` across nodes.** Rejected: couples to ADK session semantics and breaks for `BYO`/`SandboxAgent` nodes — defeating heterogeneity.
- **Orchestrator-managed git (agent returns a patch).** Considered for tighter governance, but moving changes over A2A and re-deriving commits is heavier; v1 keeps git agent-run with the credential surfaced per-run and write-scoped.

## Limitations

- **Workspace nodes need pre-configured git egress.** Egress is deploy-time per-agent policy and can't be granted at run time, so a `ReadOnly`/`ReadWrite` node's agent must already allow the repo host; the workflow **validates** this and fails fast (no silent break). The credential is surfaced per-run by the workflow, but the push still relies on the agent actually committing/pushing — mitigated (not eliminated) by the `verifyPush` gate, which fails the node rather than corrupting the chain.
- **Run state is in-memory for the run's duration.** The state bag lives in the orchestrator; an orchestrator restart mid-run aborts the in-flight run (the pushed commits survive in git, but the run is not transparently resumed). Durable/resumable runs are future work.
- **v1 topology is a single unconditional chain, untyped state.** No conditional/parallel/loops and no schema validation (all v2 — see Forward-compatibility).
- **Structured state depends on the A2A converter.** Cross-node structured data uses `kagent_type`-tagged `DataPart`s (untagged `DataPart`s are dropped, and File/URL parts don't survive the round-trip, in kagent's current converter); registering the state type is a small converter addition.

## Open Questions

- Should the orchestrator runtime reuse the existing agent runtime image or ship as a dedicated lightweight orchestrator?
- Reserved state keys beyond `input` (e.g. `workspace.commit`) — naming and exposure.

## Future Work

Each v2 item is an additive relaxation of a v1 edge/state constraint (Forward-compatibility):

- **Conditional branching** — edges gain an optional `when` (a condition on `{node.field}`); a node may have multiple conditional out-edges.
- **Parallel** — fan-out/join via multiple unconditional out-edges, executed concurrently.
- **Loop** — a back-edge with `maxIterations` + an until-`when` (the same condition primitive as branching; ADK's `LoopAgent` is the precedent).
- **Typed state (v2):** optional JSON-Schema validation of state-bag values. kagent has no declarative output-schema field today (only A2A skill output MIME modes), so this is new orchestrator-side validation reusing the handoff's *structured-output → validate → re-prompt* loop; the fiddly part is CRD ergonomics for embedding a schema (likely `runtime.RawExtension`). Low–medium effort, deferred for YAGNI not difficulty.
- **HITL between nodes (orchestrator-owned approval gate):** a per-node `humanApproval` gate enforced at the node boundary — **approve** (advance) or **request changes** (re-prompt the same node's agent in the same A2A session with feedback → revise → re-gate, bounded by `maxRevisions`). Owned by the orchestrator — not delegated to the agent's own `ask_user` (which would make a control-flow guarantee depend on agent goodwill). It **reuses kagent's existing A2A interrupt transport** (the `tool_approval` / `input_required` interrupt carrying a `decision_type` payload): since an `AgentWorkflow` is itself an A2A endpoint, it surfaces that interrupt to the dashboard and receives the human's decision back as a tagged HITL `DataPart`. Distinct from the `Agent` CRD's tool-scoped `requireApproval`.
- **Non-git workspace transport:** **object store / artifact-URI** (e.g. S3 / Artifactory) for build outputs, datasets, and non-code workspaces — the general successor to v1's git-only transport, same pass-by-reference principle.
- **Harness-managed workspace:** transparent checkout/commit by the sandbox harness so the agent only sees a working directory.
- **Dynamic fan-out:** generate nodes from a prior node's output.
