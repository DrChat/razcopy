# (Rust) azcopy
[AzCopy](https://github.com/Azure/azure-storage-azcopy) rewritten in Rust, for the lulz and efficiency gainz.

## Features and capabilities
❌ Use with storage accounts that have a hierarchical namespace (Azure Data Lake Storage Gen2).

❌ Create containers and file shares.

❌ Upload files and directories.

✅ Download files and directories.

❌ Copy containers, directories and blobs between storage accounts (Service to Service).

✳️ Synchronize data between Local <=> Blob Storage, Blob Storage <=> File Storage, and Local <=> File Storage.

✳️ Recover from failures by restarting previous jobs.

❌ Delete blobs or files from an Azure storage account

❌ Copy objects, directories, and buckets from Amazon Web Services (AWS) to Azure Blob Storage (Blobs only).

❌ Copy objects, directories, and buckets from Google Cloud Platform (GCP) to Azure Blob Storage (Blobs only).

❌ List files in a container.
