# Project Challenges, Errors & Architectural Decisions

This document is a running log of real problems hit while building the ecommerce lakehouse pipeline, why they happened, how they were fixed, and what to say about them in an interview. Keep appending to this as the project continues, since this record is arguably more valuable on a CV than a clean build would have been, as it demonstrates real debugging methodology, not just tutorial-following.

---

## 1. PySpark 3.5.7 fails on Python 3.12 (Windows)

**Symptom:** Python worker exited unexpectedly (crashed) / EOFException when running any Spark job that spawned a Python worker process (e.g. df.show()). No Python-side traceback, just a generic JVM-side socket failure. Crash occurred consistently ~3 seconds after the worker attempted to start.

**What didn't work (in order tried):**
1. Disabling the Windows Store Python alias (app execution aliases)
2. Setting PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON explicitly
3. Adding Windows Firewall allow-rules for java.exe and python.exe
4. Forcing local[1] (single-threaded) mode
5. Disabling Spark's Python worker reuse (spark.python.worker.reuse=false)
6. Setting PYTHONUNBUFFERED=1
7. Enabling Spark's Python fault handler and DEBUG-level logging (never actually surfaced a Python-side error, the worker was dying before any Python code executed)

**Root cause:** PySpark 3.5.7's worker-spawn mechanism has a compatibility issue with Python 3.12 on Windows specifically. Confirmed by creating a parallel virtual environment on Python 3.11 and running the identical script; it worked immediately, with no other changes.

**Fix:** Standardized the entire project on Python 3.11 for any environment that runs PySpark. Consolidated producers/venv (3.12) into a single venv311 (3.11) environment.

**Lesson for the CV/interview:** When a stack trace shows only a generic, low-level failure (socket timeout, EOF, no application error) and configuration tuning isn't moving the needle, suspect a version-compatibility issue between major components. Prove it by isolating one variable completely (a parallel environment) rather than continuing to guess at configuration. This is a textbook case of the difference between "trying more settings" and "designing a test that actually isolates the variable."

---

## 2. hadoop.dll downloaded correctly but still not loadable (Windows)

**Symptom:** UnsatisfiedLinkError on NativeIO$Windows.access0 when Spark Structured Streaming tried to manage its checkpoint directory.

**What looked right but wasn't enough:**
- HADOOP_HOME was correctly set to C:\hadoop
- winutils.exe and hadoop.dll were correctly downloaded and present in C:\hadoop\bin, with correct non-zero file sizes

**Root cause:** Windows' native library loader (the JVM's System.loadLibrary call underneath Hadoop's NativeIO) requires the containing folder of a .dll to be on the system PATH, not just referenced by an unrelated environment variable like HADOOP_HOME. These are two independently necessary, easily-conflated conditions.

**Fix:** Explicitly added C:\hadoop\bin to the user PATH environment variable (not just setting HADOOP_HOME).

**Lesson:** On Windows, "the environment variable points to the right folder" and "the DLL is actually loadable by the JVM" are separate requirements. This is a known, documented Windows/Hadoop gotcha, worth searching for exact error text before assuming a version mismatch (an earlier, wrong hypothesis was that the winutils.exe/hadoop.dll version didn't match the Hadoop client version being used).

---

## 3. Iceberg catalog must be named spark_catalog for dbt-spark to work

**Symptom:** Three different, progressively-more-specific errors while wiring dbt to Iceberg:
1. REQUIRES_SINGLE_PART_NAMESPACE - dbt generated a two-part schema reference (local.bronze) that Spark's default catalog couldn't resolve
2. TABLE_OR_VIEW_NOT_FOUND - after reverting to a flat schema name, dbt could no longer find the table at all, because the Iceberg catalog config was never actually loaded into dbt's Spark session
3. Replacing a view is not supported by catalog: spark_catalog - once the catalog config was correctly loaded via profiles.yml, a new error appeared

**Root cause:** dbt-spark generates SQL using unqualified table references (no catalog prefix). Spark SQL resolves unqualified references through its default catalog, which is hardcoded to be named spark_catalog. The raw PySpark scripts in this project used an Iceberg catalog named local with fully-qualified references (local.bronze.table) everywhere, which worked fine, but dbt has no mechanism to inject that prefix automatically, so the only fix is to name the Iceberg catalog spark_catalog itself (overriding Spark's built-in default catalog implementation), confirmed by cross-referencing multiple independent sources (Iceberg documentation, a Snowplow integration guide, and an EMR/dbt integration writeup) that all independently converge on the same convention.

**Fix:** Changed spark.sql.catalog.local to spark.sql.catalog.spark_catalog in the dbt connection profile (profiles.yml, under server_side_parameters).

**Lesson:** A working PySpark setup does not guarantee a working dbt setup on the same underlying engine. dbt's SQL-generation conventions impose their own constraints beyond what the query engine itself technically requires, always check the tool's conventions, not just the engine's capabilities.

---

## 4. Iceberg doesn't support SQL views, every dbt model needs materialized='table'

**Symptom:** Replacing a view is not supported by catalog: spark_catalog.

**Root cause:** dbt's default materialization strategy is view (a lightweight CREATE VIEW in the underlying SQL engine). Iceberg's Spark catalog implementation only manages physical data tables, it has no concept of a SQL view as a first-class object.

**Fix:** Every dbt model targeting an Iceberg-backed table must explicitly set config(materialized='table') at the top of the model file. This is a hard compatibility requirement, not a style or performance choice.

---

## 5. git-filter-repo: cleaning up an accidentally-committed virtual environment

**Symptom:** git push succeeded but GitHub warned about a 56MB JAR file inside producers/venv311/, and the total push size was 310.88 MiB, far larger than a source-only repository should be.

**Root cause:** venv311 was created and git add .'d before .gitignore was updated to exclude it (the exclusion pattern only covered the old venv/ folder name). .gitignore only prevents future commits from including a path, it does nothing retroactively. One commit made in that gap permanently baked ~300MB of installed packages into git history, and simply deleting the folder or untracking it in a later commit doesn't remove the data from earlier commits; git history is additive by design.

**Fix:**
1. Identified the exact commit that introduced the bloat via git log --all --oneline -- producers/venv311
2. Confirmed no other large blobs existed via git rev-list --objects --all piped through git cat-file --batch-check and sorted by size
3. Made a disposable, fully independent clone (git clone --no-local, required because a same-machine clone hardlinks objects by default and filter-repo refuses to run against that)
4. Ran git filter-repo --path producers/venv311 --invert-paths on the disposable clone to rewrite every commit's history, stripping the offending path entirely
5. Verified the result (git count-objects -vH showed size drop from 310.88 MiB to 279.80 KiB, roughly 1100x smaller)
6. Swapped the cleaned .git folder into the real working directory (preserving all actual project files, which were untouched by the rewrite)
7. Force-pushed the rewritten history to GitHub (safe here specifically because this was a solo repository with no other collaborators who might have already pulled the old history)

**Lesson:** .gitignore is not retroactive. A brief gap between creating a new environment/folder and updating .gitignore can permanently bloat repository history unless caught and fixed with a real history-rewrite tool (git filter-repo, the modern, officially-recommended replacement for the older and riskier git filter-branch). Also: always operate history-rewriting tools on a disposable clone first, never directly on your working repository.

---

## 6. Iceberg tables are not portable across operating systems without a migration step

**Symptom:** UnsupportedFileSystemException: No FileSystem for scheme "C" when a Linux-based Airflow Docker container tried to read Bronze tables that had originally been written by Windows-based PySpark scripts.

**Root cause:** Iceberg's Hadoop-catalog implementation bakes absolute file paths directly into each table's snapshot metadata (the .avro manifest files) at write time, this is not just a catalog-level warehouse configuration setting, but literal path strings stored inside the committed metadata of every snapshot. Windows paths (C:/iceberg-warehouse/...) are structurally meaningless on Linux, which has no concept of drive letters. Changing the warehouse config in a new profile only affects where new tables get created, it cannot retroactively reinterpret paths that are already permanently recorded inside existing tables' metadata.

**Why this wasn't fixed immediately:** Two real options exist: (a) use Iceberg's rewrite_table_path procedure to migrate existing metadata in place, or (b) rebuild the affected tables from the original source (Kafka) with the correct target path from the start. Option (a) is designed for large-scale production migrations and would mean learning a fairly obscure Iceberg procedure to solve a problem affecting a handful of test rows, high effort, uncertain payoff. Option (b) is straightforward: the original Kafka topics are untouched and OS-agnostic, so re-running the already-proven streaming consumers against a Linux-native warehouse path takes a fraction of the time and carries far less risk. Decision: rebuild fresh (option b).

**Also discovered while investigating this:** The Airflow Docker Compose stack and the Kafka/Zookeeper Docker Compose stack run on separate Docker networks by default. A container in one stack cannot reach a container in the other without explicitly connecting the networks (or running a service as a one-off container attached to both). This has to be resolved before Bronze tables can be rebuilt from inside the Airflow container.

**Lesson:** This is a general, transferable truth about Iceberg (and Hadoop-backed systems generally), not specific to this project: a Hadoop-catalog Iceberg warehouse is tied to the filesystem semantics of whatever machine created it. Moving it to a genuinely different OS/filesystem root requires an explicit migration step, a mount and a config change are not sufficient. This is exactly the kind of "why does Iceberg matter" detail worth being able to explain in an interview: snapshot metadata durability is a feature (time travel, audit history) that comes with a real portability tradeoff.

---

## 7. Rebuilding Bronze from Kafka surfaced a stale malformed message

**Symptom:** After successfully rebuilding Bronze tables natively on Linux (See #6), dbt test failed three tests, not_null checks on session_id, user_id, and a downstream user_summary.user_id, each reporting exactly one bad row.

**Root cause:** A batch replay using startingOffsets: earliest reads a Kafka topic's entire retained history, including old manual test messages sent during initial Kafka setup and verification (e.g. a plain string like "hello from confluent-kafka" sent via kafka-console-producer while testing connectivity, long before the JSON producer existed). from_json fails to parse non-JSON content and returns NULL for the entire struct, not just individual fields, this propagated downstream into every dependent model and test.

**Fix:** Added a filter for isNotNull immediately after from_json parsing, using a field that is guaranteed non-null on every genuinely valid message (user_id for clickstream, order_id for order events, chosen deliberately per-topic, since order_events' legitimate partial-update rows have other nulls by design).

**Lesson:** A full-history Kafka replay is not the same as "only the data my current producer writes", a topic accumulates everything ever sent to it, including throwaway messages from earlier debugging sessions. Production data-ingestion pipelines should always filter or dead-letter unparseable records rather than assume upstream data is uniformly well-formed, even when you believe you know everything that's ever been written to a topic.

---

## 8. Snowflake signup blocked, substituted DuckDB for analytical query-engine verification

**Symptom:** Snowflake's free trial signup got stuck on a card-verification step (a $0.00 authorization confirmed on-phone that never let the signup proceed). Snowflake's own documentation states plainly that no payment information is required for the standard 30-day/$400-credit trial, so this was landing on an unexpected signup variant, not standard trial behavior, and wasn't resolved within a reasonable time budget.

**Decision:** Rather than keep troubleshooting a third-party signup flow with no visibility into why it was stuck, substituted DuckDB as the analytical query-engine layer instead of Snowflake. Alternatives considered and rejected:
- Trino: closer architectural match to Snowflake (distributed SQL engine), but requires its own Docker Compose setup and configuration, similar cost to today's Airflow work, for a component that's meant to be a secondary verification step, not the main focus.
- BigQuery: has a genuine free tier, but Google Cloud's broader signup also typically requires card details, risking the same friction.
- DuckDB: zero signup, zero account, embedded (just a Python package), with native Iceberg table support via an extension. Chosen because it eliminates external-service risk entirely while still proving the real architectural point.

**What this actually demonstrates:** DuckDB reading and correctly aggregating (GROUP BY/COUNT) the same physical Iceberg table files that Spark wrote and dbt transformed, with results reconciling exactly against known row counts (509 clickstream rows, 412 order rows, both matching precisely across event-type/status breakdowns), is direct proof of Iceberg's core value proposition: an open table format is genuinely readable by independent query engines with no shared process, coordination, or vendor lock-in. This is arguably a more convincing demonstration of "why Iceberg" than a Snowflake integration would have been, since Snowflake's own native Iceberg support is a comparatively recent, heavily-marketed feature, where DuckDB simply reading the raw Iceberg spec correctly, with zero vendor involvement, more directly proves the underlying open-format promise.

**Lesson:** When a third-party service's signup/onboarding flow becomes an unexpected blocker unrelated to the actual technical skill being demonstrated, it's worth asking whether the underlying capability (cross-engine analytical querying over an open table format) can be proven a different way, rather than spending debugging time on a vendor's account-provisioning system, which teaches nothing transferable about data engineering. Not every roadmap item needs to be completed via the originally-planned tool if a substitute demonstrates the same underlying principle.

---

## 9. Kubernetes: Confluent image rejects Kubernetes auto-injected KAFKA_PORT variable

**Symptom:** kafka-0 pod crash-looped with exit code 1 immediately after "Configuring...", with the log line "port is deprecated. Please use KAFKA_ADVERTISED_LISTENERS instead." No further output, no stack trace.

**Root cause:** Kubernetes automatically injects environment variables into every pod for each Service in the same namespace, using a legacy Docker-links naming convention. A Service literally named "kafka" caused Kubernetes to auto-generate a variable named KAFKA_PORT. Confluent's own container entrypoint script has a hardcoded check that treats any KAFKA_PORT variable as a sign of old-style deprecated configuration and immediately exits, even though this variable was never set manually. This is a known, documented interaction bug between Kubernetes auto-injection and Confluent's cp-kafka image, unrelated to the actual manifest content.

**Fix:** Renamed the Kubernetes Service from "kafka" to two separately-named services (kafka-headless for internal StatefulSet DNS, kafka-external for NodePort access) so Kubernetes never generates a variable named exactly KAFKA_PORT.

**Lesson:** Kubernetes automatic env-var injection for Services is easy to forget about and can silently collide with application-level environment variable conventions. Worth checking a container's own environment variable expectations against what Kubernetes might auto-generate before naming Services.

---

## 10. Kind clusters do not expose NodePorts to the host by default

**Symptom:** Kafka pod running and healthy inside the cluster, but connecting from a Windows-side Python client to localhost on the NodePort failed with connection-refused on both IPv4 and IPv6.

**Root cause:** Unlike Minikube or cloud-managed Kubernetes, a kind cluster runs its entire "node" inside a single Docker container. NodePorts are only exposed within that container's network by default, nothing automatically forwards them to the host machine.

**Fix:** kind requires explicit extraPortMappings in the cluster config file, set at cluster-creation time (cannot be patched into a running cluster). This binds a specific container port to the same port on the host when the node container starts.

**Lesson:** "The pod is Running and healthy" and "the service is reachable from outside the cluster" are two independent facts, verify both, especially with local Kubernetes tooling like kind where host networking behaves differently than cloud-managed clusters most tutorials assume.

---

## 11. Helm chart schema drift: guessing config field names from an old mental model fails silently

**Symptom:** First values override used a webserver.service key expecting it to expose the UI via NodePort. The dry-run rendered successfully with no errors, but the actual Service manifest showed no NodePort configuration at all, the override was silently ignored.

**Root cause:** Airflow 3.x's Helm chart renamed the "webserver" component to "apiServer" (matching Airflow 3's own architectural split into a separate API server process). Helm does not error on unrecognized keys in a values file by default, it silently ignores them.

**Fix:** Searched the actual default-values.yaml (pulled fresh via helm show values) for the real field structure, found the correct apiServer.service.type / apiServer.service.ports path, and re-verified via a second dry-run that the rendered manifest actually contained the NodePort configuration.

**Lesson:** Helm's silent-ignore behavior for unknown keys means a successful dry-run is not proof every override took effect, only that the YAML was syntactically valid. Always verify specific values actually appear in the rendered output.

---

## 12. Helm chart automated create-user Job did not produce a working login

**Symptom:** Airflow UI loaded via the NodePort, but the documented default credentials (admin/admin) returned "invalid login or password."

**Root cause:** airflow users list inside the running scheduler pod showed zero users existed. The chart's create-user Job (a post-install hook) either failed silently or completed and was cleaned up by its 300-second TTL before being investigated.

**Fix:** Ran airflow users create manually inside the scheduler pod via kubectl exec, using the same credentials the chart's Job was supposed to create automatically.

**Lesson:** Helm chart post-install Jobs with short TTLs can complete, fail, or get cleaned up before you notice something is wrong. If documented default credentials do not work, check the actual application state directly rather than assuming the automated step succeeded.

---

## 13. Terraform and cloud account setup (AWS)

**Decision:** Provisioned a real AWS S3 bucket via Terraform (versioning enabled, public access explicitly blocked) to serve as a genuine cloud-backed Iceberg warehouse location, rather than using Terraform against only local Kubernetes resources. Terraform's actual value proposition is cloud infrastructure provisioning, and using it exclusively against local tooling would be a non-standard demonstration.

**Practices followed:**
- Created a dedicated IAM user (terraform-deploy) with a scoped S3-only policy rather than root credentials or broad AdministratorAccess, least-privilege from the start.
- AWS credentials configured via the AWS CLI credentials file, never hardcoded into any .tf file or committed to git.
- .terraform/ and *.tfstate / *.tfstate.backup explicitly excluded from version control; only main.tf and the provider lock file committed, matching Terraform's own guidance on source control versus remote state.

**Lesson:** Real, minimal cloud infrastructure with correct security practices (scoped IAM, gitignored state, no hardcoded secrets) is a stronger demonstration of production-relevant skill than avoiding cloud entirely or over-provisioning beyond what the project needs.

## Status: Project roadmap complete

Every item from the original technical stack has a real, working, verified implementation: Kafka (with hand-rolled Kubernetes StatefulSets), Spark Structured Streaming, Apache Iceberg (Medallion architecture, Bronze/Silver/Gold), dbt Core, Docker, Kubernetes (both hand-rolled manifests and a production-pattern Helm deployment with live git-sync), and Terraform against real AWS infrastructure. Snowflake was substituted with DuckDB after a signup blocker (see #8); this remains the one deliberate substitution from the original plan, fully documented and defensible on its own technical merits.
