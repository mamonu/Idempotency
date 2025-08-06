---
marp: true
theme: default
paginate: true
header: 'Data Engineering Design Patterns - Chapter 4'
footer: 'Idempotency Design Patterns'
style: |
  section {
    font-size: 20px;
  }
  h1 {
    color: #2563eb;
    font-size: 32px;
  }
  h2 {
    color: #1e40af;
    font-size: 24px;
  }
  h3 {
    color: #3730a3;
    font-size: 18px;
  }
  code {
    background-color: #f3f4f6;
    padding: 2px 6px;
    border-radius: 3px;
  }
  pre {
    background-color: #1f2937;
    color: #f9fafb;
  }
  .columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
  }
---

# Idempotency Design Patterns
## Chapter 4: Data Engineering Design Patterns

**Ensuring Consistency in Data Processing** written by *Bartosz Konieczny*

Presented by *Theodore Manassis*

---

# Agenda

1. **Introduction to Idempotency**
2. **Overwriting Patterns**
   - Fast Metadata Cleaner
   - Data Overwrite
3. **Update Patterns**
   - Merger
   - Stateful Merger
4. **Database Patterns**
   - Keyed Idempotency
   - Transactional Writer
5. **Immutable Dataset Pattern**
   - Proxy
6. **Key Takeaways**

---

# The Challenge

## Why Idempotency Matters

- **Automatic recovery** from failures can lead to data duplication
- **Retried tasks** might replay successful write operations
- **Best case**: Removable duplicates
- **Worst case**: Unidentifiable duplicate data → nightmare scenario!

> *"Each data engineering activity eventually leads to errors"*

---

# What is Idempotency?

## Definition
**Idempotency**: No matter how many times you run an operation, you get the same result

### Example: Absolute Function
```python
absolute(-1) == absolute(absolute(absolute(-1)))
# Always returns 1
```

### In Data Engineering
- Ensures consistent output without duplicates
- Enables safe retries and backfilling
- Critical for data quality and reliability

---

# Pattern Categories

### 1. Overwriting Family
- Fast Metadata Cleaner
- Data Overwrite

### 2. Updates Family
- Merger
- Stateful Merger

### 3. Database Family
- Keyed Idempotency
- Transactional Writer

### 4. Immutable Dataset
- Proxy

---

# Pattern 1: Fast Metadata Cleaner

## Problem
- Daily batch job processing 500GB - 1.5TB
- DELETE operation performance degrades as table grows
- Need scalable idempotent solution

## Solution
Use metadata operations instead of data operations:
- `TRUNCATE TABLE` - faster than DELETE
- `DROP TABLE` - completely removes table
- Partition data into smaller, manageable units

---

# Fast Metadata Cleaner - Implementation

<div class="columns">
<div>

### Key Concepts
- **Idempotency granularity**
- Physical isolation of datasets
- Logical data exposition (views)

</div>
<div>

### Workflow Steps
1. Analyze execution date
2. Create idempotency environment
3. Update data exposition layer
4. Load new data

</div>
</div>

```sql
-- Fast operation (metadata)
TRUNCATE TABLE visits_week_42;

-- Vs slow operation (data)
DELETE FROM visits WHERE week = 42;
```

---

# Fast Metadata Cleaner - Consequences

## ✅ Advantages
- Very fast operations
- Clear idempotency boundaries
- Scalable approach

## ⚠️ Limitations
- **Granularity boundary**: Must backfill entire partition
- **Metadata limits**: 4,000 partitions (BigQuery), 200,000 tables (Redshift)
- **Schema evolution** challenges
- Requires data exposition layer (views)

---

# Pattern 2: Data Overwrite

## When to Use
- No metadata layer available (e.g., object stores)
- Need simple overwrite semantics
- Full dataset available each run

## Implementation Options

### Data Processing Frameworks
```python
# Apache Spark
input_data.write.mode('overwrite').text(output_path)
```

### SQL Operations
```sql
INSERT OVERWRITE INTO devices 
SELECT * FROM devices_staging;
```

---

# Pattern 3: Merger

## Problem
- Incremental changes from CDC (Change Data Capture)
- Need to maintain current state
- Must handle inserts, updates, and deletes

## Solution: MERGE Operation
```sql
MERGE INTO devices AS target
USING changed_devices AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

---

# Merger - Handling Deletes

## Challenge
Merger pattern doesn't naturally support hard deletes

## Solution: Soft Deletes
```sql
MERGE INTO devices AS target
USING changes AS source
ON target.id = source.id
WHEN MATCHED AND source.is_deleted = true 
  THEN DELETE
WHEN MATCHED AND source.is_deleted = false 
  THEN UPDATE SET ...
WHEN NOT MATCHED AND source.is_deleted = false 
  THEN INSERT ...
```

---

# Pattern 4: Stateful Merger

## Problem with Basic Merger
During backfilling, incremental datasets become inconsistent

## Solution
Add state management:
1. **State table** tracks versions
2. **Restore mechanism** for backfilling
3. **Version tracking** per execution

---

Add state management pt2
### State Table Structure
| Execution Time | Table Version |
|---------------|---------------|
| 2024-10-05    | 1             |
| 2024-10-06    | 2             |
| 2024-10-07    | 5 (backfilled)|

---

# Stateful Merger - Implementation

<div class="columns">
<div>

### Workflow
1. Check if backfilling
2. Restore table if needed
3. Run MERGE operation
4. Update state table

---

# Stateful Merger - Implementation pt2

### Backfilling Detection
```python
if previous_version < 
   last_merge_version:
    # Backfilling scenario
    restore_table(version)
else:
    # Normal run
    proceed_with_merge()
```

---

# Pattern 5: Keyed Idempotency

## Concept
Generate deterministic keys for records

## Key Generation Strategy
Use **immutable attributes**:
- Append time (not event time!)
- Execution time for batch jobs
- Unique business identifiers

## Example: Session Generation
```python
session_id = hash(str(min_append_time))
# Same input → Same key → Idempotent writes
```

---

# Keyed Idempotency - Considerations

## ✅ Works Well For:
- NoSQL databases (Cassandra, HBase)
- Key-value stores
- File/partition naming

## ⚠️ Challenges:
- **Relational databases**: Need MERGE instead of INSERT
- **Apache Kafka**: Eventual deduplication via compaction
- **Mutable sources**: Keys may change if source data changes

---

# Pattern 6: Transactional Writer

## Problem
- Spot/preemptible instances cause task failures
- Consumers see partial or duplicate data
- Need all-or-nothing semantics

## Solution
Leverage database transactions:
1. **BEGIN** transaction
2. **WRITE** data
3. **COMMIT** on success / **ROLLBACK** on failure

---

# Transactional Writer - Implementation

### Apache Flink + Kafka Example
```python
kafka_sink = (KafkaSink.builder()
    .set_delivery_guarantee(
        DeliveryGuarantee.EXACTLY_ONCE
    )
    .set_property('transaction.timeout.ms', 
                  str(10 * 60 * 1000))
    .build())
```

### Key Points
- Consumers only see committed data
- Provides exactly-once semantics
- Requires transaction support in target system

---

# Pattern 7: Proxy (Immutable Dataset)

## Problem
- Legal requirement to keep all historical versions
- Need to expose only latest version
- Must maintain immutability

## Solution
1. **Write-once tables** with versioning/timestamps
2. **Proxy layer** (view) exposes latest version
3. **Access control** ensures immutability

---

# Proxy Pattern - Implementation

### Approaches
1. **View-based**
   - Versioned tables
   - Single access view
   
2. **Manifest-based**
   - Files with manifests
   - Reference latest version

3. **Native versioning**
   - Delta Lake/Iceberg
   - Time travel feature

---

# Proxy Pattern - Implementation
### Example
```sql
-- Versioned table
CREATE TABLE devices_v_20241105
  (LIKE devices);

-- Proxy view
CREATE VIEW devices AS
SELECT * FROM 
  devices_v_20241105;
```
---

# Choosing the Right Pattern

## Decision Tree

```
Full Dataset Available?
├─ YES → Overwriting Patterns
│   ├─ Have Metadata Layer? → Fast Metadata Cleaner
│   └─ No Metadata? → Data Overwrite
└─ NO (Incremental)
    ├─ Need Backfilling? → Stateful Merger
    ├─ Simple Updates? → Merger
    └─ Streaming/Keys? → Keyed Idempotency
```

**Special Cases:**
- Need transactions? → Transactional Writer
- Must be immutable? → Proxy

---

# Best Practices

## 1. **Choose Based on Your Context**
   - Data volume and velocity
   - Infrastructure capabilities
   - Business requirements

## 2. **Consider the Trade-offs**
   - Performance vs. consistency
   - Complexity vs. maintainability
   - Storage costs vs. operational simplicity

## 3. **Test Your Idempotency**
   - Simulate failures
   - Verify backfilling scenarios
   - Monitor for duplicates

---

# Common Pitfalls to Avoid

## 🚫 Don't Forget:

1. **Granularity impacts backfilling**
   - Weekly partitions = weekly backfills

2. **Metadata has limits**
   - Check your platform's constraints

3. **Transactions aren't free**
   - Added latency for coordination
   - Not all systems support them

4. **Keys must be truly immutable**
   - Event time can change (late data)
   - Use append/ingestion time instead

---

# Real-World Implementation Example

## Scenario: Daily Sales Pipeline

```python
# Apache Airflow DAG
def choose_idempotency_path(**context):
    ex_date = context['execution_date']
    if ex_date.day_of_week == 1:  # Monday
        return 'create_weekly_table'
    return 'append_to_table'

# Branch based on execution context
router = BranchPythonOperator(
    task_id='idempotency_router',
    python_callable=choose_idempotency_path
)
```

---

# Key Takeaways

## 1. **Idempotency is Essential**
Without it, retries lead to data quality nightmares

## 2. **Multiple Approaches Exist**
Choose based on your specific requirements

## 3. **Trade-offs Are Inevitable**
Balance performance, consistency, and complexity

## 4. **Test Thoroughly**
Especially backfilling and failure scenarios

## 5. **Combine with Error Management**
Chapter 3 + Chapter 4 = Robust pipelines

---

# Summary

## Idempotency Patterns Provide:

✅ **Consistency** - Same result every time
✅ **Reliability** - Safe retries and backfilling  
✅ **Quality** - No mysterious duplicates
✅ **Peace of mind** - Sleep better at night!

## Remember:
> *"We can solve any problem by introducing an extra level of indirection"*
> — The Proxy Pattern philosophy

---

# Questions & Discussion

## Topics for Deep Dive:

- Which pattern fits your current challenges?
- How do you handle idempotency today?
- What are your biggest data consistency pain points?

### Resources:
- Book: "Data Engineering Design Patterns" by Bartosz Konieczny
- Article: "Functional Data Engineering" by Maxime Beauchemin
- Iceberg/Athena/dbt documentation

---

# Thank You!

## Next Steps:

1. **Assess** your current pipelines for idempotency gaps
2. **Choose** appropriate patterns for your use cases
3. **Implement** incrementally with proper testing
4. **Monitor** for duplicates and consistency issues
5. **Iterate** and improve based on learnings

### Contact & Resources
📧 [theodoros.manassis@justice.gov.uk]
🔗 [https://github.com/bartosz25/data-engineering-design-patterns-book/]
📚 Next time: Chapter 5 - Data Value Patterns