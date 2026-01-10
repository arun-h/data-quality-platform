
from metadata.store import MetadataStore

store = MetadataStore()
print("Metadata store created successfully!")

# Test checkpoint operations
store.update_checkpoint("test_dataset", "2024-01-01")
last = store.get_last_checkpoint("test_dataset")
print(f"Last checkpoint: {last}")

store.close()