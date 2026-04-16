import os
import random
import pandas as pd
import numpy as np

# ----------------------------
# CONFIGURATION
# ----------------------------
NUM_ROWS = 1_000_000
NUM_VERSIONS = 50
UPDATE_RATIO = 0.01
BRANCH_PROB = 0.2
OUTPUT_DIR = "sci_data"

random.seed(42)
np.random.seed(42)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ----------------------------
# GLOBAL STATE
# ----------------------------
versions = {}
version_parents = {}
next_id = NUM_ROWS  # for inserts

# ----------------------------
# CREATE BASE TABLE
# ----------------------------
def create_base_table(n):
    return pd.DataFrame({
        "id": np.arange(n),
        "col1": np.random.randint(0, 1000, size=n),
        "col2": np.random.rand(n),
        "col3": np.random.choice(["A", "B", "C", "D"], size=n)
    })

base_df = create_base_table(NUM_ROWS)
versions[0] = base_df
version_parents[0] = None

base_df.to_csv(f"{OUTPUT_DIR}/version_0.csv", index=False)
print("Created base version (v0)")

# ----------------------------
# MODIFY TABLE (WITH DELTAS)
# ----------------------------
def modify_table_with_deltas(parent_df):
    global next_id

    df = parent_df.copy()
    n = len(df)
    num_changes = int(n * UPDATE_RATIO)

    inserts = []
    updates = []
    deletes = []
    rows_to_delete = []

    indices = np.random.choice(df.index, size=num_changes, replace=False)

    for idx in indices:
        op = random.choice(["update", "delete", "insert"])

        if op == "update":
            old_row = df.loc[idx].copy()

            df.at[idx, "col1"] = random.randint(0, 1000)
            df.at[idx, "col2"] = random.random()

            new_row = df.loc[idx]

            updates.append({
                "id": old_row["id"],
                "old_col1": old_row["col1"],
                "old_col2": old_row["col2"],
                "old_col3": old_row["col3"],
                "new_col1": new_row["col1"],
                "new_col2": new_row["col2"],
                "new_col3": new_row["col3"],
            })

        elif op == "delete":
            row = df.loc[idx]
            deletes.append(row.to_dict())
            rows_to_delete.append(idx)

        elif op == "insert":
            new_row = {
                "id": next_id,
                "col1": random.randint(0, 1000),
                "col2": random.random(),
                "col3": random.choice(["A", "B", "C", "D"])
            }
            inserts.append(new_row)
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            next_id += 1

    # Delete all marked rows at once
    df = df.drop(rows_to_delete, errors='ignore')

    return df, inserts, updates, deletes

# ----------------------------
# GENERATE VERSIONS
# ----------------------------
for v in range(1, NUM_VERSIONS):

    # Choose parent
    if random.random() < BRANCH_PROB:
        parent = random.choice(list(versions.keys()))
    else:
        parent = v - 1

    parent_df = versions[parent]

    new_df, inserts, updates, deletes = modify_table_with_deltas(parent_df)

    versions[v] = new_df
    version_parents[v] = parent

    # Save deltas
    pd.DataFrame(inserts).to_csv(f"{OUTPUT_DIR}/v{v}_inserts.csv", index=False)
    pd.DataFrame(updates).to_csv(f"{OUTPUT_DIR}/v{v}_updates.csv", index=False)
    pd.DataFrame(deletes).to_csv(f"{OUTPUT_DIR}/v{v}_deletes.csv", index=False)

    print(f"Version {v} | parent={parent} | +{len(inserts)} ~{len(updates)} -{len(deletes)}")

# ----------------------------
# SAVE VERSION GRAPH
# ----------------------------
graph_df = pd.DataFrame([
    {"version": v, "parent": version_parents[v]}
    for v in version_parents
])

graph_df.to_csv(f"{OUTPUT_DIR}/version_graph.csv", index=False)

print("Saved version graph")