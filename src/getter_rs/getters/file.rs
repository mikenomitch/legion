use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::getter_rs::getters::Getter;
use crate::getter_rs::Source;

#[derive(Debug, Default)]
pub struct FileGetter;

impl FileGetter {
    pub const SCHEME: &'static str = "file";

    pub fn new() -> Self {
        Self
    }
}

impl Getter for FileGetter {
    fn scheme(&self) -> &'static str {
        Self::SCHEME
    }

    fn get(&self, source: &Source, dest: &Path) -> Result<PathBuf> {
        let source_path = match source {
            Source::Path(path) => path,
            Source::Url(_) => return Err(anyhow!("file getter expects a local path")),
        };

        let metadata = fs::metadata(source_path)
            .with_context(|| format!("read metadata for {}", source_path.display()))?;

        if metadata.is_dir() {
            let dest_dir = prepare_directory_destination(dest)?;
            copy_dir(source_path, &dest_dir)?;
            return Ok(dest_dir);
        }

        let dest_path = prepare_file_destination(dest, source_path)?;
        fs::copy(source_path, &dest_path).with_context(|| {
            format!(
                "copy file from {} to {}",
                source_path.display(),
                dest_path.display()
            )
        })?;
        Ok(dest_path)
    }
}

fn prepare_directory_destination(dest: &Path) -> Result<PathBuf> {
    if dest.exists() {
        if dest.is_dir() {
            return Ok(dest.to_path_buf());
        }
        return Err(anyhow!("destination is not a directory"));
    }

    fs::create_dir_all(dest).with_context(|| format!("create directory {}", dest.display()))?;
    Ok(dest.to_path_buf())
}

fn prepare_file_destination(dest: &Path, source_path: &Path) -> Result<PathBuf> {
    if dest.exists() && dest.is_dir() {
        let file_name = source_path
            .file_name()
            .ok_or_else(|| anyhow!("source file has no name"))?;
        return Ok(dest.join(file_name));
    }

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create directory {}", parent.display()))?;
    }

    Ok(dest.to_path_buf())
}

fn copy_dir(source: &Path, dest: &Path) -> Result<()> {
    for entry in
        fs::read_dir(source).with_context(|| format!("read directory {}", source.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let dest_path = dest.join(entry.file_name());
        if file_type.is_dir() {
            fs::create_dir_all(&dest_path)?;
            copy_dir(&path, &dest_path)?;
        } else {
            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&path, &dest_path).with_context(|| {
                format!(
                    "copy file from {} to {}",
                    path.display(),
                    dest_path.display()
                )
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn copy_file_into_destination_dir() {
        let source_dir = tempdir().unwrap();
        let source_path = source_dir.path().join("source.txt");
        fs::write(&source_path, "hello").unwrap();

        let dest_dir = tempdir().unwrap();
        let getter = FileGetter::new();

        let dest_path = getter
            .get(&Source::Path(source_path.clone()), dest_dir.path())
            .unwrap();

        assert_eq!(dest_path, dest_dir.path().join("source.txt"));
        assert_eq!(fs::read_to_string(dest_path).unwrap(), "hello");
    }
}
