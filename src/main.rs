#![feature(slice_from_ptr_range, file_set_times)]
use std::{
    fs::FileTimes,
    path::{Path, PathBuf},
    sync::atomic::AtomicUsize,
};

use anyhow::{bail, Context};
use azure_storage::prelude::*;
use azure_storage_blobs::{blob::BlobProperties, container::operations::BlobItem, prelude::*};
use bitvec::vec::BitVec;
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar};
use memmap2::MmapOptions;
use tokio::io::AsyncReadExt;
use url::Url;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Synchronize a file from remote storage to local storage
    Sync {
        /// The remote storage URL to synchronize from
        remote_url: Url,
        /// The local destination
        local_path: PathBuf,
    },
}

#[allow(dead_code)]
mod style {
    use indicatif::ProgressStyle;

    pub fn style_prog() -> ProgressStyle {
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {wide_bar:.cyan/blue} {pos:>10}/{len:10} ({eta}) {msg}")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    }

    pub fn style_prog_bytes() -> ProgressStyle {
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {wide_bar:.cyan/blue} {bytes:>12}/{total_bytes:12} {bytes_per_sec} ({eta}) {msg}")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    }

    pub fn style_bytes() -> ProgressStyle {
        ProgressStyle::default_bar()
            .template(
                "[{elapsed_precise}] {bar:.cyan/blue} {bytes:>12}/{total_bytes:12} {wide_msg}",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    }
}

fn parse_blob_name(blob_name: impl AsRef<str>) -> PathBuf {
    let n = blob_name.as_ref();
    let c = n.split('/');

    let mut p = PathBuf::new();
    for c in c {
        p.push(c);
    }

    p
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum VerificationResult {
    Verified,
    Mismatch,
    Unknown,
}

/// Verify if a file matches a blob
async fn verify_file(
    p: impl AsRef<Path>,
    blob_props: &BlobProperties,
    pb: Option<&ProgressBar>,
) -> anyhow::Result<VerificationResult> {
    let f = tokio::fs::File::open(p.as_ref())
        .await
        .context("failed to open file")?;

    let meta = f
        .metadata()
        .await
        .context("failed to query file metadata")?;

    if let Some(md5) = &blob_props.content_md5 {
        use md5::{Context, Digest};
        use tokio::io::BufReader;

        let mut context = Context::new();

        // Read in file from disk and calculate md5.
        let mut buf = [0u8; 8192];
        let mut r = BufReader::new(f);

        if let Some(pb) = pb {
            pb.set_length(meta.len());
        }

        while let Ok(size) = r.read(&mut buf).await {
            if size == 0 {
                break;
            }

            if let Some(pb) = pb {
                pb.inc(size as u64);
            }

            context.consume(&buf[..size]);
        }

        let digest = context.compute();
        if digest == Digest(*md5.as_slice()) {
            Ok(VerificationResult::Verified)
        } else {
            Ok(VerificationResult::Mismatch)
        }
    } else {
        // No serverside hash exposed.
        // Try to compare length.
        if meta.len() != blob_props.content_length {
            Ok(VerificationResult::Mismatch)
        } else {
            Ok(VerificationResult::Unknown)
        }
    }
}

/// Attempt to download a blob from an Azure storage account.
///
/// This expects that you've already sized the file to the blob's final size.
async fn get_blob(
    client: BlobClient,
    props: &BlobProperties,
    f: &mut std::fs::File,
    skip: Option<u64>,
    prog: impl Fn(u64),
) -> anyhow::Result<()> {
    const DEFAULT_CHUNK_SIZE: u64 = 0x1000 * 0x1000;

    let map = MmapOptions::new()
        .map_raw(&*f)
        .context("failed to map file into memory")?;

    let map_range = (map.as_mut_ptr() as usize, map.len());

    // `skip` should be a multiple of the chunk size.
    let skip = skip.unwrap_or(0);
    let skip_chunks = skip / DEFAULT_CHUNK_SIZE;

    let b = client.get().chunk_size(DEFAULT_CHUNK_SIZE);

    let b = if skip != 0 {
        // Update progress to reflect skipped contents.
        prog(skip);

        b.range(skip..props.content_length)
    } else {
        b
    };

    let body = b.into_stream();

    // Track the chunks that were downloaded, and if the download
    // fails then truncate the file to the last contiguous chunk.
    let mut chunks = BitVec::<AtomicUsize, bitvec::order::Lsb0>::new();
    chunks.resize(
        ((props.content_length + (DEFAULT_CHUNK_SIZE - 1)) / DEFAULT_CHUNK_SIZE) as usize,
        false,
    );

    // Fill in previously downloaded chunks.
    if skip != 0 {
        chunks
            .get_mut(..skip_chunks as usize)
            .expect("invalid number of skip chunks")
            .fill(true);
    }

    // FIXME: Not sure about the concurrency limit.
    // Too many, and Azure throws authentication errors (instead of rate limit errors?)
    let r = body
        .try_for_each_concurrent(Some(32), |mut r| {
            let prog = &prog;
            let chunks = &chunks;
            let map_range = &map_range;
            // let map = &map;

            async move {
                let map_range = map_range.clone();

                let range = r
                    .content_range
                    .map(|r| r.start as usize..(r.end + 1) as usize)
                    .unwrap_or(0usize..r.blob.properties.content_length as usize);
                let mut offs = 0usize;

                // SAFETY: Who knows, probably unsafe?
                // Technically more than one thread should not have mutable access to memory,
                // but in practice everyone's writing to a blob of bytes.
                let map_slice =
                    unsafe { std::slice::from_raw_parts_mut(map_range.0 as *mut u8, map_range.1) };
                let map_slice = &mut map_slice[range.clone()];

                while let Some(chunk) = r
                    .data
                    .try_next()
                    .await
                    .map_err(|e| e.context(format!("failed to read chunk at {offs:#018X}")))?
                {
                    prog(chunk.len() as u64);

                    map_slice[(offs as usize)..(offs as usize) + chunk.len()]
                        .copy_from_slice(&chunk[..]);

                    offs += chunk.len();
                }

                // Flush the range of bytes we just wrote, ignoring any errors.
                // FIXME: Need to do this on a blocking worker instead. The system likes to
                // block this operation.
                // let _ = map.flush_async_range(range.start, range.end - range.start);

                let chunk = range.start / (DEFAULT_CHUNK_SIZE as usize);
                chunks.set_aliased(chunk, true);

                Ok(())
            }
        })
        .await;

    match r {
        Ok(_) => {}
        Err(e) => {
            let c = chunks
                .first_zero()
                .expect(&format!("no chunks failed to download? {e:#}")) as u64;
            let offs = c * DEFAULT_CHUNK_SIZE;

            // Need to unmap the file before resizing it.
            drop(map);

            f.set_len(offs).context("failed to truncate file")?;
            return Err(e).context("failed to download chunks");
        }
    }

    // Assert that every chunk was downloaded.
    assert!(chunks.all(), "{chunks:?}");

    map.flush_async().context("failed to flush mapped file")?;
    drop(map);

    Ok(())
}

async fn sync_blobs(
    client: ContainerClient,
    blobs: &[Blob],
    target: PathBuf,
) -> anyhow::Result<()> {
    // Tally up total bytes that we'll be downloading...
    let total_bytes = blobs
        .iter()
        .fold(0u64, |a, b| a + b.properties.content_length);

    // Create target folder.
    std::fs::create_dir_all(&target).context("failed to create target directory")?;

    let m = MultiProgress::new();

    // Display overall progress.
    let pb = m.add(ProgressBar::new(total_bytes).with_style(style::style_prog_bytes()));

    // TODO: Clean out extraneous files.

    // Create download tasks for each file.
    let downloads = futures::stream::iter(blobs.into_iter().map(|blob| {
        // Capture variables from the outer function.
        let client = client.clone();
        let m = m.clone();
        let pb = pb.clone();
        let target = target.clone();

        async move {
            let blob = blob.clone();
            let blob_name = blob.name.clone();

            // Try and see if there is already a file on disk, and whether or not we should
            // replace it.
            let blob_path = parse_blob_name(&blob_name);
            let file_path = target.join(&blob_path);

            std::fs::create_dir_all(file_path.parent().unwrap())
                .context("failed to create relative path")?;

            tokio::spawn(async move {
                let temp_file_path = file_path.with_file_name(format!(
                    "{}.tmpdownload",
                    file_path.file_name().unwrap().to_string_lossy()
                ));

                let should_download = if file_path.exists() {
                    let vpb = m.add(
                        ProgressBar::new(0)
                            .with_style(style::style_bytes())
                            .with_message(format!("verify {}", &blob.name)),
                    );

                    let result = verify_file(&file_path, &blob.properties, Some(&vpb))
                        .await
                        .context("failed to verify existing file")?;

                    match result {
                        VerificationResult::Verified => {
                            vpb.finish_and_clear();
                            false
                        }
                        VerificationResult::Mismatch => {
                            // vpb.finish_with_message(format!("failed to verify {}", &blob.name));
                            vpb.finish_and_clear();
                            true
                        }
                        VerificationResult::Unknown => {
                            vpb.finish_and_clear();
                            false // TODO: Should we or should we not redownload unknown files?
                        }
                    }
                } else {
                    true
                };

                if should_download {
                    let len = blob.properties.content_length;

                    let fpb = m.add(
                        ProgressBar::new(len)
                            .with_style(style::style_bytes())
                            .with_message(format!("{}", &blob.name)),
                    );

                    // Query existing file for metadata (if any)
                    let (past_len, trunc) = if let Ok(meta) = std::fs::metadata(&temp_file_path) {
                        // If file length on disk is equal to the blob's length, just truncate it
                        // since it was likely left over from a previous crash or something.
                        if meta.len() == len {
                            (None, true)
                        } else {
                            (Some(meta.len()), false)
                        }
                    } else {
                        // No existing file.
                        (None, true)
                    };

                    let mut f = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(trunc)
                        .open(&temp_file_path)
                        .context("failed to create file for download")?;

                    f.set_len(len).context("failed to set file length")?;

                    // The length can be 0 sometimes...
                    // If it is, then the get request for the blob will fail.
                    if len != 0 {
                        let blob_client = client.blob_client(&blob.name);
                        match get_blob(blob_client, &blob.properties, &mut f, past_len, |len| {
                            pb.inc(len);
                            fpb.inc(len);
                        })
                        .await
                        {
                            Ok(()) => {}
                            Err(e) => {
                                fpb.finish_with_message(format!("{} failed: {}", &blob.name, &e));
                                return Err(e).context("failed to get blob")?;
                            }
                        }
                    }

                    // Setup file times.
                    let ft = FileTimes::new();
                    let ft = if let Some(access_time) = &blob.properties.last_access_time {
                        ft.set_accessed((*access_time).into())
                    } else {
                        ft
                    };

                    let ft = ft.set_modified(blob.properties.last_modified.into());
                    f.set_times(ft).context("failed to set file times")?;

                    // Rename the file to its destination name.
                    if file_path.exists() {
                        tokio::fs::remove_file(&file_path)
                            .await
                            .context("failed to delete original file")?;
                    }

                    tokio::fs::rename(&temp_file_path, &file_path)
                        .await
                        .context("failed to rename file")?;

                    fpb.set_message(format!("verify {}", &blob.name));
                    fpb.set_position(0);

                    match verify_file(&file_path, &blob.properties, Some(&fpb))
                        .await
                        .context("failed to verify written file")?
                    {
                        VerificationResult::Unknown | VerificationResult::Verified => {
                            fpb.finish_and_clear();

                            Ok(())
                        }
                        VerificationResult::Mismatch => {
                            bail!("downloaded file but verification failed")
                        }
                    }
                } else {
                    pb.inc(blob.properties.content_length);

                    Ok(())
                }
            })
            .await
            .context(format!("failed to join file task for {}", &blob_name))?
            .context(format!("failed to download file {}", &blob_name))
        }
    }))
    .buffer_unordered(64)
    .collect::<Vec<Result<(), anyhow::Error>>>()
    .await;

    for r in downloads {
        if let Err(e) = r {
            log::error!("{e:?}");
        }
    }

    pb.finish();

    Ok(())
}

async fn run() -> anyhow::Result<()> {
    env_logger::init();

    // sync <remote-url> <local-folder>
    // - SAS URL: fetch blob names if container
    //   - URL scheme: https://<account>.blob.core.windows.net/<container>/<file?>?<SAS-parameters>
    //   - https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs?tabs=azure-ad
    //   -
    //
    let args = Args::parse();

    match args.command {
        Commands::Sync {
            remote_url,
            local_path,
        } => {
            // Determine the account.
            let account = if let Some(domain) = remote_url.domain() {
                // Split out the subdomain.
                if let Some(subdomain) = domain.split('.').next() {
                    subdomain
                } else {
                    bail!("could not parse domain: {domain}");
                }
            } else {
                bail!("unsupported URL: {remote_url}");
            };

            // Determine if the URL is an SAS URL.
            let creds = if remote_url.query_pairs().any(|(a, _)| a == "sig") {
                // This is an SAS URL.
                // FIXME: Somehow avoid that unwrapping?
                StorageCredentials::sas_token(remote_url.query().unwrap())
                    .context("failed to parse SAS token")?
            } else {
                todo!()
            };

            let client = ClientBuilder::new(account, creds);

            let mut segments = remote_url
                .path_segments()
                .context("SAS URL has no path segments")?;
            let container = segments.next().context("no container specified")?;
            let file = segments.next();

            if let Some(_file) = file {
                // TODO: Downloading a single file...
                todo!()
            } else {
                let client = client.container_client(container);
                let blob_items = client
                    .list_blobs()
                    .into_stream()
                    .map_ok(|b| {
                        // HACK: Not really sure why I have to map the inner here, but
                        // we quickly get into trait hell if it isn't mapped to a Result<_>.
                        futures::stream::iter(
                            b.blobs.items.into_iter().map(|b| Ok::<_, anyhow::Error>(b)),
                        )
                    })
                    .try_flatten()
                    .try_collect::<Vec<_>>()
                    .await?;

                let blobs = blob_items
                    .into_iter()
                    .filter_map(|b| {
                        if let BlobItem::Blob(b) = b {
                            Some(b)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                sync_blobs(client, &blobs, local_path)
                    .await
                    .context("failed to sync blobs")?;
            }

            Ok(())
        }
    }
}

/// Main stub. Used to avoid macro shenanigans with rust-analyzer.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await
}
