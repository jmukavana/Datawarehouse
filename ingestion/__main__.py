from .pipeline import run

if __name__ == "__main__":
    counts = run()
    if not counts:
        print("No new partitions to load.")
    for table, count in sorted(counts.items()):
        print(f"  {table:45s} {count:7d} rows")
