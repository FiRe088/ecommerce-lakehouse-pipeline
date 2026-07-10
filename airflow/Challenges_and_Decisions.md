# Project Challenges, Errors & Architectural Decisions

This document is a running log of real problems hit while building the ecommerce lakehouse pipeline, why they happened, how they were fixed, and what to say about them in an interview. Keep appending to this as the project continues — this record is arguably more valuable on a CV than a clean build would have been, since it demonstrates real debugging methodology, not just tutorial-following.

---

## 1. PySpark 3.5.7 fails on Python 3.12 (Windows)

**Symptom:** `Python worker exited unexpectedly (crashed)` / `EOFException` when running any Spark job that spawned a Python worker process (e.g. `df.show()`). No Python-side traceback, just a generic JVM-side socket failure. Crash occurred consistently ~3 seconds after the worker attempted to start.

**What didn't work (in order tried):**
1. Disabling the Windows Store Python alias (`app execution aliases`)
2. Setting `PYSPARK_PYTHON` / `PYSPARK_DRIVER_PYTHON` explicitly
3. Adding Windows Firewall allow-rules for `java.exe` and `python.exe`
4. Forcing `local[1]` (single-threaded) mode
5. Disabling Spark's Python worker reuse (`spark.python.worker.reuse=false`)
6. Setting `PYTHONUNBUFFERED=1`
7. Enabling Spark's Python fault handler and DEBUG-level logging (never actually surfaced a Python-side error — the worker was dying before any Python code executed)

**Root cause:** PySpark 3.5.7's worker-spawn mechanism has a compatibility issue with Python 3.12 on Windows specifically. Confirmed by creating a parallel virtual environment on Python 3.11 and running the identical script — it worked immediately, with no other changes.

**Fix:** Standardized the entire project on **Python 3.11** for any environment that runs PySpark. Consolidated `producers/venv` (3.12) into a single `venv311` (3.11) environment.

**Lesson for the CV/interview:** When a stack trace shows only a generic, low-level failure (socket timeout, EOF, no application error) and configuration tuning isn't moving the needle, suspect a version-compatibility issue between major components. Prove it by isolating one variable completely (a parallel environment) rather than continuing to guess at configuration. This is a textbook case of the difference between "trying more settings" and "designing a test that actually isolates the variable."

---

## 2. `hadoop.dll` downloaded correctly but still not loadable (Windows)

**Symptom:** `UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(...)'` when Spark Structured Streaming tried to manage its checkpoint directory.

**What looked right but wasn't enough:**
- `HADOOP_HOME` was correctly set to `C:\hadoop`
- `winutils.exe` and `hadoop.dll` were correctly downloaded and present in `C:\hadoop\bin`, with correct non-zero file sizes

**Root cause:** Windows' native library loader (the JVM's `System.loadLibrary` call underneath Hadoop's `NativeIO`) requires the **containing folder** of a `.dll` to be on the system `PATH` — not just referenced by an unrelated environment variable like `HADOOP_HOME`. These are two independently necessary, easily-conflated conditions.

**Fix:** Explicitly added `C:\hadoop\bin` to the user `PATH` environment variable (not just setting `HADOOP_HOME`).

**Lesson:** On Windows, "the environment variable points to the right folder" and "the DLL is actually loadable by the JVM" are separate requirements. This is a known, documented Windows/Hadoop gotcha — worth searching for exact error text before assuming a version mismatch (an earlier, wrong hypothesis was that the `winutils.exe`/`hadoop.dll` version didn't match the Hadoop client version being used).

---

## 3. Iceberg catalog must be named `spark_catalog` for dbt-spark to work

**Symptom:** Three different, progressively-more-specific errors while wiring dbt to Iceberg:
1. `REQUIRES_SINGLE_PART_NAMESPACE` — dbt generated a two-part schema reference (`local.bronze`) that Spark's default catalog couldn't resolve
2. `TABLE_OR_VIEW_NOT_FOUND` — after reverting to a flat schema name, dbt could no longer find the table at all, because the Iceberg catalog config was never actually loaded into dbt's Spark session
3. `Replacing a view is not supported by catalog: spark_catalog` — once the catalog config was correctly loaded via `profiles.yml`, a new error appeared

**Root cause:** dbt-spark generates SQL using **unqualified** table references (no catalog prefix). Spark SQL resolves unqualified references through its **default catalog**, which is hardcoded to be named `spark_catalog`. The raw PySpark scripts in this project used an Iceberg catalog named `local` with fully-qualified references (`local.bronze.table`) everywhere, which worked fine — but dbt has no mechanism to inject that prefix automatically, so the only fix is to name the Iceberg catalog `spark_catalog` itself (overriding Spark's built-in default catalog implementation), confirmed by cross-referencing multiple independent sources (Iceberg documentation, a Snowplow integration guide, and an EMR/dbt integration writeup) that all independently converge on the same convention.

**Fix:** Changed `spark.sql.catalog.local` → `spark.sql.catalog.spark_catalog` in the dbt connection profile (`profiles.yml`, under `server_side_parameters`).

**Lesson:** A working PySpark setup does not guarantee a working dbt setup on the same underlying engine. dbt's SQL-generation conventions impose their own constraints beyond what the query engine itself technically requires — always check the *tool's* conventions, not just the engine's capabilities.

---

## 4. Iceberg doesn't support SQL views — every dbt model needs `materialized='table'`

**Symptom:** `Replacing a view is not supported by catalog: spark_catalog`.

**Root cause:** dbt's default materialization strategy is `view` (a lightweight `CREATE VIEW` in the underlying SQL engine). Iceberg's Spark catalog implementation only manages physical data tables — it has no concept of a SQL view as a first-class object.

**Fix:** Every dbt model targeting an Iceberg-backed table must explicitly set `{{ config(materialized='table') }}` at the top of the model file. This is a hard compatibility requirement, not a style or performance choice.

---

## 5. `git-filter-repo`: cleaning up an accidentally-committed virtual environment

**Symptom:** `git push` succeeded but GitHub warned about a 56MB JAR file inside `producers/venv311/`, and the total push size was 310.88 MiB — far larger than a source-only repository should be.

**Root cause:** `venv311` was created and `git add .`'d **before** `.gitignore` was updated to exclude it (the exclusion pattern only covered the old `venv/` folder name). `.gitignore` only prevents *future* commits from including a path — it does nothing retroactively. One commit made in that gap permanently baked ~300MB of installed packages into git history, and simply deleting the folder or untracking it in a later commit doesn't remove the data from earlier commits; git history is additive by design.

**Fix:**
1. Identified the exact commit that introduced the bloat via `git log --all --oneline -- producers/venv311`
2. Confirmed no other large blobs existed via `git rev-list --objects --all | git cat-file --batch-check=... | sort by size`
3. Made a disposable, fully independent clone (`git clone --no-local`, required because a same-machine clone hardlinks objects by default and `filter-repo` refuses to run against that)
4. Ran `git filter-repo --path producers/venv311 --invert-paths` on the disposable clone to rewrite every commit's history, stripping the offending path entirely
5. Verified the result (`git count-objects -vH` showed size drop from 310.88 MiB to 279.80 KiB — roughly 1100x smaller)
6. Swapped the cleaned `.git` folder into the real working directory (preserving all actual project files, which were untouched by the rewrite)
7. Force-pushed the rewritten history to GitHub (safe here specifically because this was a solo repository with no other collaborators who might have already pulled the old history)

**Lesson:** `.gitignore` is not retroactive. A brief gap between creating a new environment/folder and updating `.gitignore` can permanently bloat repository history unless caught and fixed with a real history-rewrite tool (`git filter-repo`, the modern, officially-recommended replacement for the older and riskier `git filter-branch`). Also: always operate history-rewriting tools on a disposable clone first, never directly on your working repository.

---

## 6. Iceberg tables are not portable across operating systems without a migration step

**Symptom:** `org.apache.hadoop.fs.UnsupportedFileSystemException: No FileSystem for scheme "C"` when a Linux-based Airflow Docker container tried to read Bronze tables that had originally been written by Windows-based PySpark scripts.

**Root cause:** Iceberg's Hadoop-catalog implementation bakes **absolute file paths** directly into each table's snapshot metadata (the `.avro` manifest files) at write time — this is not just a catalog-level `warehouse` configuration setting, but literal path strings stored inside the committed metadata of every snapshot. Windows paths (`C:/iceberg-warehouse/...`) are structurally meaningless on Linux, which has no concept of drive letters. Changing the `warehouse` config in a new profile only affects where **new** tables get created — it cannot retroactively reinterpret paths that are already permanently recorded inside existing tables' metadata.

**Why this wasn't fixed immediately:** Two real options exist — (a) use Iceberg's `rewrite_table_path` procedure to migrate existing metadata in place, or (b) rebuild the affected tables from the original source (Kafka) with the correct target path from the start. Option (a) is designed for large-scale production migrations and would mean learning a fairly obscure Iceberg procedure to solve a problem affecting a handful of test rows — high effort, uncertain payoff. Option (b) is straightforward: the original Kafka topics are untouched and OS-agnostic, so re-running the already-proven streaming consumers against a Linux-native warehouse path takes a fraction of the time and carries far less risk. **Decision: rebuild fresh (option b).**

**Also discovered while investigating this:** The Airflow Docker Compose stack and the Kafka/Zookeeper Docker Compose stack run on **separate Docker networks** by default. A container in one stack cannot reach a container in the other without explicitly connecting the networks (or running a service as a one-off container attached to both). This has to be resolved before Bronze tables can be rebuilt from inside the Airflow container.

**Lesson:** This is a general, transferable truth about Iceberg (and Hadoop-backed systems generally), not specific to this project: a Hadoop-catalog Iceberg warehouse is tied to the filesystem semantics of whatever machine created it. Moving it to a genuinely different OS/filesystem root requires an explicit migration step — a mount and a config change are not sufficient. This is exactly the kind of "why does Iceberg matter" detail worth being able to explain in an interview: snapshot metadata durability is a feature (time travel, audit history) that comes with a real portability tradeoff.

---

## Open / Next Steps
- Connect the Airflow and Kafka Docker networks (`docker network connect`, or add both services to a shared external network in their respective `docker-compose.yaml` files)
- Rebuild Bronze tables from Kafka, targeting `/opt/iceberg-warehouse` (the Linux/container-native path) as the single source of truth going forward
- Decide: should dbt/Spark development happen exclusively inside the container from this point on (simpler, one environment), or should Windows-host and container environments be treated as intentionally separate, each with their own independently-rebuildable data (more flexible for local iteration, more moving parts to maintain)?
- Remaining roadmap items from the original project plan: Kubernetes orchestration, Snowflake integration, Terraform for infrastructure-as-code, Airflow DAG scheduling for the full Bronze→Silver→Gold pipeline end-to-end