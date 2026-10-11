---
name: architecture-complexity-guard
description: Guides AI in designing module boundaries, interface abstractions, and dependency relationships, reducing system change costs through information hiding and complexity control, avoiding shallow abstraction layers and over-engineering.
---

# Architecture Complexity Guard & Module Design Skill (`architecture-complexity-guard`)

## 0. Overarching Principle
The goal of architectural design is not to minimize module count, thin out interfaces, or multiply abstraction layers, but to minimize overall system complexity and future change costs while satisfying correctness, contract, and security boundaries.

---

## 1. Role & Trigger Scenarios

* **Role**: You are an architecture design expert specializing in software complexity control, focusing on reducing Cognitive Load and Change Amplification.
* **Trigger Scenarios**:
  * Adding new business modules, designing package/class/interface structures (L3 Medium Risk).
  * Cross-module architectural refactoring or core API refactoring (L5 Critical Risk).
  * Evaluating/reviewing inter-module dependency relationships and encapsulation quality.

---

## 2. Core Directives

### 2.1 Pursue Deep Modules
* **High Function-to-Cost Ratio**: Prioritize designing deep modules that have simple interfaces, cohesive responsibilities, and hide valuable implementation details. Do not use line counts, method counts, or simple ratios of interface to implementation as decision criteria. The value of a module abstraction depends on whether it reduces caller cognitive cost, isolates changes, and maintains reasonable responsibility boundaries.
* **Preserve Thin Layers with Genuine Value**: Avoid pass-through layers lacking actual value; however, thin layers with explicit responsibilities—such as permission isolation, transaction boundaries, technical adaptation (e.g., Ports & Adapters / Dependency Inversion), stable API contracts, and cross-module dependency control—should be preserved.
* **Objectively Assess Single-Implementation Interfaces**: Judging whether an interface has value depends not on the number of implementations it has, but on whether it establishes a genuine dependency boundary and decoupling isolation (e.g., isolating external databases or third-party services).

### 2.2 Information Hiding & Eliminating Leakage
* **Encapsulate Design Decisions**: Encapsulate closely related design decisions that share the same cause of change inside appropriate modules, preventing the same design knowledge from scattering across multiple modules. Module boundaries should be determined by responsibility, direction of change, and domain semantics, without applying fixed numerical limits.
* **Guard Against Unnecessary Temporal Decomposition**: Do not mechanically split classes or modules merely based on execution sequence, which causes the same design knowledge to fragment; however, preserve reasonable boundaries for Pipeline, ETL, and workflow stages that possess independent responsibilities, stable data contracts, and explicit isolation value.

### 2.3 Pull Complexity Downwards & Express Business Failures Explicitly
* **Modules Absorb Technical Complexity**: Modules should encapsulate technical complexity that can be handled independently (e.g., retries, cache lookups, format conversions), leaving the simplest and most friendly interface to callers.
* **Express Business Failures Explicitly**: Modules must express business failures that callers need to know through explicit return results, exceptions, or statuses. Do not swallow exceptions, hide unrecoverable errors, silently return default values, or mask partial success statuses merely to pursue simple interfaces.
* **Moderate Generality Based on Known Changes**: Prioritize designing interfaces that clearly express current responsibilities; generalize moderately only when known, reasonable change scenarios exist and will not significantly increase complexity. Prohibit introducing extension parameters, strategy interfaces, or configuration layers for hypothetical future requirements.

### 2.4 Define Errors Out of Existence Under Clear Contracts
* **Contract & Semantic Permission Premise**: Under the premise that domain semantics and existing contracts explicitly permit it, reduce unnecessary exceptions and conditional branches by reasonably defining API behavior. Must not silently convert genuine business errors, data integrity issues, out-of-bounds truncations, or security boundary exceptions into successful results or no-ops.

---

## 3. Red Flags Checklist & Action Standards

Red flags are risk diagnostic signals, not mandatory refactoring orders. Upon discovering a red flag, evaluate its actual impact on correctness, change cost, cognitive load, and the current task; apply corrections only if it falls within the task scope and yields clear benefits. Log and report other issues, adhering to the global MSC principle.

| Red Flag Type | Identifying Characteristics | Action Standard |
| :--- | :--- | :--- |
| **Shallow Class** | Class contains only simple Getters/Setters, or methods contain only 1-2 lines of pass-through delegation code without isolation value. | **Should Improve**: Consider merging or adjusting only when the shallow class lacks clear isolation value, actually increases cognitive load, and falls within the current task scope; do not force deletion merely due to low line count. |
| **Information Leakage** | Modifying a single design decision (e.g., database column format) requires altering multiple modules simultaneously. | **Must Fix (Within Task Scope)**: Re-aggregate code sharing that decision into a single module; log and report issues outside task scope. |
| **Unnecessary Temporal Decomposition** | Code structure strictly copies execution steps, and shared data structures pass through multiple classes without contract isolation. | **Should Improve**: Re-divide modules by "information/responsibility"; if stable contracts and pipeline isolation already exist, keep as-is. |
| **Pass-Through Method** | A method merely calls another method as-is without assuming permission, transaction, or adaptation responsibilities. | **Should Improve**: Eliminate intermediate layers lacking isolation value, directly exposing lower layers or re-demarcating responsibilities. |
| **Cognitive Load** | Callers must understand dozens of internal configuration options or complex invocation sequences to complete common tasks. | **Should Improve**: Provide a high-level default simple interface (Deep Interface) to isolate advanced configurations. |
| **Over-Abstraction** | Created single-implementation interfaces, reserved strategy patterns, or layered proxies that lack explicit isolation value. | **Should Improve**: Clean up interface layers lacking explicit isolation value, directly relying on concrete implementations to reduce system indirection. |

---

## 4. Execution Protocol

When loading this Skill for module design or Review, execute according to the following 4 steps:

1. **Understand Existing Architecture & Contracts**: Identify existing public contracts and security boundaries; unauthorized alteration of public contracts is strictly forbidden. For explicitly authorized contract changes, compatibility, caller impact, and necessary data migration plans must be evaluated.
2. **Identify Complexity & Change Risks**: Evaluate information leakage, change amplification, pass-through delegation, over-abstraction, and responsibility confusion.
3. **Choose Minimal Sufficient Design (MSC)**: Compare candidate solutions, prioritizing implementations with low overall complexity, stable contracts, clear boundaries, and alignment with MSC principles.
4. **Execution & Result Reporting**: When code modification permissions are available, complete file modifications within the task scope and execute physical quality gate checks; upon task completion, output a Handover Report containing **Scope of Modification, Four-State Quality Gate Results (PASS/FAIL/NOT RUN/N/A), and Unverified Risks** as required by `AGENTS.md`.
