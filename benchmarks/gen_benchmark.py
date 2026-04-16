import os
import random
import pandas as pd
import numpy as np

# ----------------------------
# CONFIGURATION
# ----------------------------
NUM_ROWS = 1_000_000        # SCI_1M equivalent
NUM_VERSIONS = 50
UPDATE_RATIO = 0.01         # 1% rows modified per version
BRANCH_PROB = 0.2           # probability of branching
OUTPUT_DIR = "sci_data"

random.seed(42)
np.random.seed(42)

# ----------------------------
# SETUP
# ----------------------------
os.makedirs(OUTPUT_DIR, exist_ok=True)

versions = {}  # version_id -> dataframe
version_parents = {}  # version_id -> parent_id

# ----------------------------
# CREATE BASE TABLE
# ----------------------------
def create_base_table(n):
    df = pd.DataFrame({
        "id": np.arange(n),
        "col1": np.random.randint(0, 1000, size=n),
        "col2": np.random.rand(n),
        "col3": np.random.choice(["A", "B", "C", "D"], size=n)
    })
    return df

base_df = create_base_table(NUM_ROWS)
versions[0] = base_df
version_parents[0] = None

# Save base
base_df.to_csv(f"{OUTPUT_DIR}/version_0.csv", index=False)

print("Created base version (v0)")

# ----------------------------
# APPLY MODIFICATIONS
# ----------------------------
def modify_table(df):
    df = df.copy()
    n = len(df)
    num_changes = int(n * UPDATE_RATIO)

    # Choose random rows to modify
    indices = np.random.choice(df.index, size=num_changes, replace=False)

    # Collect rows to delete
    rows_to_delete = []
    
    # Randomly pick operation type
    for idx in indices:
        op = random.choice(["update", "delete", "insert"])

        if op == "update":
            if idx in df.index:
                df.at[idx, "col1"] = random.randint(0, 1000)
                df.at[idx, "col2"] = random.random()
                df.at[idx, "col3"] = random.choice(["A", "B", "C", "D"])

        elif op == "delete":
            rows_to_delete.append(idx)

        elif op == "insert":
            new_row = {
                "id": df["id"].max() + 1,
                "col1": random.randint(0, 1000),
                "col2": random.random(),
                "col3": random.choice(["A", "B", "C", "D"])
            }
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    # Delete all marked rows at once
    df = df.drop(rows_to_delete, errors='ignore')
    
    df.reset_index(drop=True, inplace=True)
    return df

# ----------------------------
# GENERATE VERSIONS
# ----------------------------
for v in range(1, NUM_VERSIONS):
    # Choose parent (branching logic)
    if random.random() < BRANCH_PROB:
        parent = random.choice(list(versions.keys()))
    else:
        parent = v - 1

    parent_df = versions[parent]

    new_df = modify_table(parent_df)

    versions[v] = new_df
    version_parents[v] = parent

    # Save CSV
    new_df.to_csv(f"{OUTPUT_DIR}/version_{v}.csv", index=False)

    print(f"Created version {v} (parent={parent}, rows={len(new_df)})")

# ----------------------------
# SAVE VERSION GRAPH
# ----------------------------
graph_df = pd.DataFrame([
    {"version": v, "parent": version_parents[v]}
    for v in version_parents
])

graph_df.to_csv(f"{OUTPUT_DIR}/version_graph.csv", index=False)

print("Saved version graph")